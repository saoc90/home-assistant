"""Support for sending data to an Kusto database."""
from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
import io
import json
import logging
import queue
import threading
import time
from typing import Any

from azure.kusto.data import DataFormat, KustoConnectionStringBuilder
from azure.kusto.ingest import IngestionProperties, QueuedIngestClient
import voluptuous as vol

from homeassistant.const import EVENT_HOMEASSISTANT_STOP, EVENT_STATE_CHANGED
from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.helpers import event as event_helper
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entityfilter import INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA
from homeassistant.helpers.typing import ConfigType

from .const import (
    BATCH_BUFFER_SIZE,
    BATCH_TIMEOUT,
    CATCHING_UP_MESSAGE,
    COMPONENT_CONFIG_SCHEMA_CONNECTION,
    CONF_COMPONENT_CONFIG,
    CONF_COMPONENT_CONFIG_DOMAIN,
    CONF_COMPONENT_CONFIG_GLOB,
    CONF_DEFAULT_MEASUREMENT,
    CONF_IGNORE_ATTRIBUTES,
    CONF_MEASUREMENT_ATTR,
    CONF_OVERRIDE_MEASUREMENT,
    CONF_RETRY_COUNT,
    CONF_TAGS,
    CONF_TAGS_ATTRIBUTES,
    DEFAULT_MEASUREMENT_ATTR,
    DOMAIN,
    EVENT_NEW_STATE,
    KUSTO_CONF_AAD_APP_ID,
    KUSTO_CONF_AAD_APP_KEY,
    KUSTO_CONF_AAD_AUTHORITY_ID,
    KUSTO_CONF_CONNECTIONSTRING,
    QUEUE_BACKLOG_SECONDS,
    RESUMED_MESSAGE,
    RETRY_DELAY,
    RETRY_INTERVAL,
    RETRY_MESSAGE,
    WROTE_MESSAGE,
)

_LOGGER = logging.getLogger(__name__)


def validate_version_specific_config(conf: dict) -> dict:
    """Validate configuration stub."""
    return conf


_CUSTOMIZE_ENTITY_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_OVERRIDE_MEASUREMENT): cv.string,
        vol.Optional(CONF_IGNORE_ATTRIBUTES): vol.All(cv.ensure_list, [cv.string]),
    }
)

_INFLUX_BASE_SCHEMA = INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA.extend(
    {
        vol.Optional(CONF_RETRY_COUNT, default=0): cv.positive_int,
        vol.Optional(CONF_DEFAULT_MEASUREMENT): cv.string,
        vol.Optional(CONF_MEASUREMENT_ATTR, default=DEFAULT_MEASUREMENT_ATTR): vol.In(
            ["unit_of_measurement", "domain__device_class", "entity_id"]
        ),
        vol.Optional(CONF_OVERRIDE_MEASUREMENT): cv.string,
        vol.Optional(CONF_TAGS, default={}): vol.Schema({cv.string: cv.string}),
        vol.Optional(CONF_TAGS_ATTRIBUTES, default=[]): vol.All(
            cv.ensure_list, [cv.string]
        ),
        vol.Optional(CONF_IGNORE_ATTRIBUTES, default=[]): vol.All(
            cv.ensure_list, [cv.string]
        ),
        vol.Optional(CONF_COMPONENT_CONFIG, default={}): vol.Schema(
            {cv.entity_id: _CUSTOMIZE_ENTITY_SCHEMA}
        ),
        vol.Optional(CONF_COMPONENT_CONFIG_GLOB, default={}): vol.Schema(
            {cv.string: _CUSTOMIZE_ENTITY_SCHEMA}
        ),
        vol.Optional(CONF_COMPONENT_CONFIG_DOMAIN, default={}): vol.Schema(
            {cv.string: _CUSTOMIZE_ENTITY_SCHEMA}
        ),
    }
)

INFLUX_SCHEMA = vol.All(
    _INFLUX_BASE_SCHEMA.extend(COMPONENT_CONFIG_SCHEMA_CONNECTION),
    validate_version_specific_config,
)

CONFIG_SCHEMA = vol.Schema(
    {DOMAIN: INFLUX_SCHEMA},
    extra=vol.ALLOW_EXTRA,
)


def _generate_event_to_json(conf: dict) -> Callable[[Event], dict[Any, Any]]:
    """Build event to json converter and add to config."""

    def event_to_json(event: Event) -> dict:
        """Convert event into json in format Kusto expects."""
        state: State = event.data.get(EVENT_NEW_STATE, State)
        return state.as_dict()

    return event_to_json


@dataclass
class KustoClient:
    """An InfluxDB client wrapper for V1 or V2."""

    data_repositories: list[str]
    write: Callable[[str], None]
    query: Callable[[str, str], list[Any]]
    close: Callable[[], None]


def get_kusto_connection(conf, test_write=False, test_read=False):
    """Create the kusto client."""

    client = QueuedIngestClient(
        KustoConnectionStringBuilder.with_aad_application_key_authentication(
            conf[KUSTO_CONF_CONNECTIONSTRING],
            conf[KUSTO_CONF_AAD_APP_ID],
            conf[KUSTO_CONF_AAD_APP_KEY],
            conf[KUSTO_CONF_AAD_AUTHORITY_ID],
        )
    )
    return client


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the InfluxDB component."""
    conf = config[DOMAIN]
    try:
        kusto = get_kusto_connection(conf, test_write=True)
    except ConnectionError as exc:
        _LOGGER.error(RETRY_MESSAGE, exc)
        event_helper.call_later(hass, RETRY_INTERVAL, lambda _: setup(hass, config))
        return True

    event_to_json = _generate_event_to_json(conf)
    max_tries = conf.get(CONF_RETRY_COUNT)
    instance = hass.data[DOMAIN] = KustoThread(hass, kusto, event_to_json, max_tries)
    instance.start()

    def shutdown(event):
        """Shut down the thread."""
        instance.queue.put(None)
        instance.join()

    hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, shutdown)

    return True


class KustoThread(threading.Thread):
    """A threaded event handler class."""

    def __init__(self, hass, kusto, event_to_json, max_tries):
        """Initialize the listener."""
        threading.Thread.__init__(self, name=DOMAIN)
        self.queue = queue.Queue()
        self.kusto = kusto
        self.event_to_json = event_to_json
        self.max_tries = max_tries
        self.write_errors = 0
        self.shutdown = False
        hass.bus.listen(EVENT_STATE_CHANGED, self._event_listener)

    @callback
    def _event_listener(self, event):
        """Listen for new messages on the bus and queue them for Influx."""
        item = (time.monotonic(), event)
        self.queue.put(item)

    @staticmethod
    def batch_timeout():
        """Return number of seconds to wait for more events."""
        return BATCH_TIMEOUT

    def get_events_json(self):
        """Return a batch of events formatted for writing."""
        queue_seconds = QUEUE_BACKLOG_SECONDS + self.max_tries * RETRY_DELAY

        count = 0
        events = []

        dropped = 0

        with suppress(queue.Empty):
            while len(events) < BATCH_BUFFER_SIZE and not self.shutdown:
                timeout = None if count == 0 else self.batch_timeout()
                item = self.queue.get(timeout=timeout)
                count += 1

                if item is None:
                    self.shutdown = True
                else:
                    timestamp, event = item
                    age = time.monotonic() - timestamp

                    if age < queue_seconds:
                        event_json = self.event_to_json(event)
                        if event_json:
                            events.append(event_json)
                    else:
                        dropped += 1

        if dropped:
            _LOGGER.warning(CATCHING_UP_MESSAGE, dropped)

        return count, events

    def write_to_kusto(self, events_json):
        """Write preprocessed events to influxdb, with retry."""
        for retry in range(self.max_tries + 1):
            try:
                ingestion_props = IngestionProperties(
                    database="ha",
                    table="states",
                    data_format=DataFormat.MULTIJSON,
                )

                multiline_json = "".join(
                    json.dumps(entry) + "\n" for entry in events_json
                )

                self.kusto.ingest_from_stream(
                    io.StringIO(multiline_json),
                    ingestion_properties=ingestion_props,
                )

                if self.write_errors:
                    _LOGGER.error(RESUMED_MESSAGE, self.write_errors)
                    self.write_errors = 0

                _LOGGER.debug(WROTE_MESSAGE, len(events_json))
                break
            except ValueError as err:
                _LOGGER.error(err)
                break
            except ConnectionError as err:
                if retry < self.max_tries:
                    time.sleep(RETRY_DELAY)
                else:
                    if not self.write_errors:
                        _LOGGER.error(err)
                    self.write_errors += len(events_json)

    def run(self):
        """Process incoming events."""
        while not self.shutdown:
            count, events = self.get_events_json()
            if events:
                self.write_to_kusto(events)
            for _ in range(count):
                self.queue.task_done()

    def block_till_done(self):
        """Block till all events processed."""
        self.queue.join()
