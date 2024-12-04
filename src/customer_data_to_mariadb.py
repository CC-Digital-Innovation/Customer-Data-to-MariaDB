from datetime import datetime
from itertools import batched
import os
import time

from dotenv import load_dotenv
from loguru import logger
import mariadb
from mariadb.connections import Connection as MariaDBConnection
import mariadb.cursors
import pysnow
import pytz
from pytz import timezone
import requests


# ====================== Environment / Global Variables =======================
load_dotenv(override=True)

# Initialize MariaDB constant global variables.
MARIADB_USERNAME = os.getenv('MARIADB_USERNAME')
MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD')
MARIADB_IP_ADDRESS = os.getenv('MARIADB_IP_ADDRESS')
MARIADB_DATABASE_NAME = os.getenv('MARIADB_DATABASE_NAME')
MARIADB_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
MARIADB_EXECUTE_MANY_LIMIT = 1000
MARIADB_OPSGENIE_TABLE_NAME = 'Global_Opsgenie_Alerts_Dev'
MARIADB_OPSGENIE_TABLE_COLUMNS = [
    'acknowledged', 'alias', 'count', 'created_at', 'integration_id',
    'integration_name', 'integration_type', 'is_seen', 'last_occurred_at',
    'message', 'owner', 'owner_id', 'priority', 'report_ack_time',
    'report_acknowledged_by', 'report_close_time', 'report_closed_by',
    'snoozed', 'snoozed_until', 'source', 'status', 'tags', 'tiny_id',
    'updated_at', 'id'
]
MARIADB_OPSGENIE_INSERT_PLACEHOLDERS = ', '.join(['?' for _ in MARIADB_OPSGENIE_TABLE_COLUMNS])
MARIADB_OPSGENIE_UPDATE_PLACEHOLDERS = ' = ?, '.join(MARIADB_OPSGENIE_TABLE_COLUMNS[:-1])
MARIADB_SERVICENOW_TABLE_NAME = 'Global_ServiceNow_Tickets_Dev'
MARIADB_SERVICENOW_TABLE_COLUMNS = [
    'assigned_to', 'assignment_group', 'caller_id', 'catalog_item', 'category',
    'closed_at', 'closed_by', 'cmdb_ci_name', 'company', 'created_at',
    'location', 'milestone', 'number', 'opened_by', 'priority',
    'request_number', 'requested_for', 'risk', 'severity', 'short_description',
    'source_of_level_of_effort', 'state', 'subcategory', 'ticket_type_table',
    'time_worked', 'updated_at', 'updated_by', 'urgency', 'sys_id'
]
MARIADB_SERVICENOW_INSERT_PLACEHOLDERS = ', '.join(['?' for _ in MARIADB_SERVICENOW_TABLE_COLUMNS])
MARIADB_SERVICENOW_UPDATE_PLACEHOLDERS = ' = ?, '.join(MARIADB_SERVICENOW_TABLE_COLUMNS[:-1])

# Initialize Opsgenie constant global variables.
OPSGENIE_API_KEY = os.getenv('OPSGENIE_API_KEY')
OPSGENIE_BASE_API_URL = 'https://api.opsgenie.com/v2'
OPSGENIE_MAX_RESPONSE_LIMIT = 100
OPSGENIE_MAX_ENDPOINT_LIMIT = 20000
OPSGENIE_TAG_SEPARATOR = '@@@'
OPSGENIE_ALERT_CREATED_AT_INDEX = 3

# Initialize ServiceNow constant global variables.
SERVICENOW_INSTANCE_NAME = os.getenv('SERVICENOW_INSTANCE_NAME')
SERVICENOW_USERNAME = os.getenv('SERVICENOW_USERNAME')
SERVICENOW_PASSWORD = os.getenv('SERVICENOW_PASSWORD')
SERVICENOW_CLIENT = pysnow.Client(
    instance=SERVICENOW_INSTANCE_NAME,
    user=SERVICENOW_USERNAME,
    password=SERVICENOW_PASSWORD
)
SERVICENOW_CLIENT.parameters.display_value = 'all'
SERVICENOW_CLIENT.parameters.exclude_reference_link = True
SERVICENOW_TICKET_FIELDS = [
    'assigned_to', 'assignment_group', 'caller_id', 'cat_item', 'category',
    'closed_at', 'closed_by', 'cmdb_ci', 'company', 'sys_created_on', 'location',
    'u_milestone', 'number', 'opened_by', 'priority', 'request', 'requested_for',
    'risk', 'severity', 'short_description', 'u_source_of_level_of_effort',
    'state', 'subcategory', 'sys_class_name', 'time_worked', 'sys_updated_on',
    'sys_updated_by', 'urgency', 'sys_id'
]
SERVICENOW_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SERVICENOW_MAX_RESPONSE_LIMIT = 1000


# ================================= Functions =================================
def paginate_opsgenie_alerts(query: str):
    """
    Generator function that will keep returning results from the provided query 
    until it reaches the Opsgenie maximum offset + limit combination. If there
    are more alerts to paginate, a new call to this function will need to be
    made with a different query. The results are in the form of a list of 
    dictionaries. Each dictionary represents 1 alert. Each page can be a 
    maximum of the Opsgenie maximum return limit.

    Args:
        query (str): The query to send to the Opsgenie alerts API endpoint.

    Returns:
        Iterable: An empty list if there are no results or the first (and only)
                page of results.

    Yields:
        Iterable: A page of results.
    """
    
    # Keep track of the offset from the results for pagination.
    current_offset = 0

    # Get the first page of the response.
    opsgenie_list_alerts_raw_response = requests.get(
        url=f'{OPSGENIE_BASE_API_URL}/alerts',
        headers={
            'Authorization': f'GenieKey {OPSGENIE_API_KEY}'
        },
        params={
            'query': query,
            'sort': 'createdAt',
            'order': 'desc',
            'offset': current_offset,
            'limit': OPSGENIE_MAX_RESPONSE_LIMIT
        }
    )
    opsgenie_list_alerts_response = opsgenie_list_alerts_raw_response.json()

    # Check if there is no alert data.
    if opsgenie_list_alerts_response.get('data', None) is None:
        return []

    # Check if there is not a next page.
    if opsgenie_list_alerts_response['paging'].get('next', None) is None:
        # Return the first (and only) page of alert data.
        return opsgenie_list_alerts_response['data']
    
    # Return the first page of data.
    yield opsgenie_list_alerts_response['data']

    # While there are more pages, keep paginating the alerts response.
    current_offset += OPSGENIE_MAX_RESPONSE_LIMIT
    while opsgenie_list_alerts_response['paging'].get('next', None) is not None and \
        current_offset + OPSGENIE_MAX_RESPONSE_LIMIT <= OPSGENIE_MAX_ENDPOINT_LIMIT:
        # Get the next page of the alerts response.
        opsgenie_list_alerts_raw_response = requests.get(
            url=opsgenie_list_alerts_response['paging']['next'],
            headers={
                'Authorization': f'GenieKey {OPSGENIE_API_KEY}'
            }
        )
        opsgenie_list_alerts_response = opsgenie_list_alerts_raw_response.json()
        
        # Return the next page of data.
        yield opsgenie_list_alerts_response['data']
        
        # Get the offset for the next page.
        current_offset += OPSGENIE_MAX_RESPONSE_LIMIT


def alert_dict_to_mariadb_tuple(alert_dict: dict) -> tuple:
    """
    Converts an Opsgenie alert dictionary from a response payload into a tuple
    that is formatted to be compatible with MariaDB.

    Args:
        alert_dict (dict): A dictionary from the Opsgenie alerts API response.

    Returns:
        tuple: A tuple of the Opsgenie alert dictionary that is compatible with
            MariaDB.
    """
    
    # Format and return the dictionary into a tuple that is compatible with
    # MariaDB.
    alert_tuple = (
        None if alert_dict.get('acknowledged', None) is None else f'{alert_dict['acknowledged']}',
        alert_dict.get('alias', None),
        alert_dict.get('count', None),
        None if alert_dict.get('createdAt', None) is None else \
            datetime.fromisoformat(alert_dict['createdAt']).strftime(MARIADB_DATETIME_FORMAT),
        None if alert_dict.get('integration', None) is None else alert_dict['integration'].get('id', None),
        None if alert_dict.get('integration', None) is None else alert_dict['integration'].get('name', None),
        None if alert_dict.get('integration', None) is None else alert_dict['integration'].get('type', None),
        None if alert_dict.get('isSeen', None) is None else f'{alert_dict['isSeen']}',
        None if alert_dict.get('lastOccurredAt', None) is None else \
            datetime.fromisoformat(alert_dict['lastOccurredAt']).strftime(MARIADB_DATETIME_FORMAT),
        alert_dict.get('message', None),
        None if alert_dict.get('owner', None) is None or alert_dict.get('owner') == '' else \
            alert_dict.get('owner'),
        None if alert_dict.get('ownerTeamId', None) is None or alert_dict.get('ownerTeamId') == '' else \
            alert_dict.get('ownerTeamId'),
        alert_dict.get('priority', None),
        None if alert_dict.get('report', None) is None else alert_dict['report'].get('ackTime', None),
        None if alert_dict.get('report', None) is None else alert_dict['report'].get('acknowledgedBy', None),
        None if alert_dict.get('report', None) is None else alert_dict['report'].get('closeTime', None),
        None if alert_dict.get('report', None) is None else alert_dict['report'].get('closedBy', None),
        None if alert_dict.get('snoozed', None) is None else f'{alert_dict['snoozed']}',
        None if alert_dict.get('snoozedUntil', None) is None else \
            datetime.fromisoformat(alert_dict['snoozedUntil']).strftime(MARIADB_DATETIME_FORMAT),
        alert_dict.get('source', None),
        alert_dict.get('status', None),
        OPSGENIE_TAG_SEPARATOR.join(alert_dict['tags']),
        alert_dict.get('tinyId', None),
        None if alert_dict.get('updatedAt', None) is None else \
            datetime.fromisoformat(alert_dict['updatedAt']).strftime(MARIADB_DATETIME_FORMAT),
        alert_dict['id']
    )
    return alert_tuple


def get_opsgenie_alerts(opsgenie_alerts_query: str) -> list[tuple]:
    """
    Returns a list of Opsgenie alerts in the form of tuples that are compatible
    with MariaDB. This list is limited to the maximum offset + limit from the
    Opsgenie alerts API endpoint, so the provided query may need to be
    cleverly modified to continue getting results beyond this limit.

    Args:
        opsgenie_alerts_query (str): The query to send to the Opsgenie alerts
            API endpoint.

    Returns:
        list[tuple]: The list of MariaDB-compatible alerts in the form of
            tuples.
    """
    
    logger.info('Getting a page of alerts from the Opsgenie API...')
    
    # Paginate over Opsgenie alerts.
    opsgenie_alerts = list[tuple]()
    for opsgenie_alerts_page in paginate_opsgenie_alerts(opsgenie_alerts_query):
        # Convert and add this alert to the list of alerts.
        for alert in opsgenie_alerts_page:
            opsgenie_alerts.append(alert_dict_to_mariadb_tuple(alert))

    # Return all the alerts.
    logger.info('A page of Opsgenie alerts have been recieved successfully!')
    return opsgenie_alerts


def insert_opsgenie_alerts_into_mariadb(mariadb_connection: MariaDBConnection, opsgenie_alerts: list[tuple]) -> int:
    """
    Inserts the provided Opsgenie alerts into MariaDB using the provided 
    MariaDB connection. This function will send the alerts in batches to avoid
    insertion errors. It returns the number of rows inserted into MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
        opsgenie_alerts (list[tuple]): The Opsgenie alerts to send to MariaDB.

    Returns:
        int: The total number of rows inserted.
    """
    
    logger.info('Sending a page of Opsgenie alerts to MariaDB...')
    
    # Initialize reference variables.
    mariadb_cursor = mariadb_connection.cursor()
    total_rows_inserted = 0
    
    # Push the Opsgenie alerts in batches to MariaDB.
    for opsgenie_alerts_batch in batched(opsgenie_alerts, MARIADB_EXECUTE_MANY_LIMIT):
        # Insert the batch into MariaDB.
        try:
            mariadb_cursor.executemany(
                f'INSERT IGNORE INTO {MARIADB_OPSGENIE_TABLE_NAME} ' \
                f'VALUES ({MARIADB_OPSGENIE_INSERT_PLACEHOLDERS})',
                opsgenie_alerts_batch
            )
            mariadb_connection.commit()
            total_rows_inserted += mariadb_cursor.rowcount
        except mariadb.Error as e:
            logger.error(f'{e}')
            logger.warning(
                'Trying this batch again, but 1 alert at a time. ' \
                'The erroring record will be printed to the console and '
                'batching will resume as normal after this batch.'
            )
            
            # For each Opsgenie alert in this batch, send the insert
            # command to MariaDB one alert at a time.
            for opsgenie_alert in opsgenie_alerts_batch:
                # Insert the alert into MariaDB.
                try:
                    mariadb_cursor.execute(
                        f'INSERT IGNORE INTO {MARIADB_OPSGENIE_TABLE_NAME} ' \
                        f'VALUES ({MARIADB_OPSGENIE_INSERT_PLACEHOLDERS})',
                        opsgenie_alert
                    )
                    mariadb_connection.commit()
                    total_rows_inserted += mariadb_cursor.rowcount
                except mariadb.Error as e:
                    logger.error(f'{e}')
                    logger.error(f'Object: {opsgenie_alert}')
                    
    logger.info('Opsgenie alerts page has been sent to MariaDB!')
    
    # Return how many rows were inserted.
    return total_rows_inserted


def mariadb_opsgenie_alerts_table_seeding(mariadb_connection: MariaDBConnection) -> None:
    """
    Seeds the provided table via the provided MariaDB connection with every
    single Opsgenie alert.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
    """
    
    logger.info(f'Seeding the Opsgenie table in MariaDB with all Opsgenie alerts...')
    
    # Get the time right now to make a timestamp to make the initial Opsgenie
    # query. The query is essentially "alerts created at or before right now".
    last_alert_timestamp = int(time.time() * 1000)
    opsgenie_alerts_query = f'createdAt <= {last_alert_timestamp}'
    
    # Get the first page of Opsgenie alerts.
    opsgenie_alerts_page = get_opsgenie_alerts(opsgenie_alerts_query)
    
    # As long as there are more pages of Opsgenie alerts, keep going.
    while len(opsgenie_alerts_page) > 0:
        # Push this page of Opsgenie alerts as batches into MariaDB.
        total_inserts = insert_opsgenie_alerts_into_mariadb(mariadb_connection, opsgenie_alerts_page)
        logger.info(f'Inserted {total_inserts} Opsgenie alerts into MariaDB!')
        
        # Parse the last alert creation datetime as a datetime object.
        last_alert_datetime = datetime.strptime(opsgenie_alerts_page[-1][OPSGENIE_ALERT_CREATED_AT_INDEX], MARIADB_DATETIME_FORMAT)
        
        # Make the datetime object timezone aware.
        last_alert_datetime_utc = last_alert_datetime.replace(tzinfo=timezone('UTC'))
        
        # Convert the datetime object into a UTC timestamp.
        last_alert_timestamp = int(last_alert_datetime_utc.timestamp() * 1000)
        
        # Make the next query to Opsgenie.
        opsgenie_alerts_query = f'createdAt < {last_alert_timestamp}'
        
        # Get the next page of Opsgenie alerts.
        opsgenie_alerts_page = get_opsgenie_alerts(opsgenie_alerts_query)
    
    logger.info(f'Opsgenie table in MariaDB has been seeded with all Opsgenie alerts!')


def to_mariadb_datetime(datetime_str: str, datetime_str_format: str) -> str | None:
    """
    Given a datetime string and its format, convert it into a
    MariaDB-compatible datetime string. If the datetime string is '' or None,
    return None.

    Args:
        datetime_str (str): The datetime string to convert.
        datetime_str_format (str): The format of the datetime string.

    Returns:
        str | None: The MariaDB compatible datetime string. None if an invalid
            datetime string was given.
    """
    
    # Check if there is no datetime provided.
    if datetime_str is None or datetime_str == '':
        return None
    
    # Format the provided datetime string into the MariaDB datetime string.
    datetime_obj = datetime.strptime(datetime_str, datetime_str_format)
    mariadb_datetime_str = datetime_obj.strftime(MARIADB_DATETIME_FORMAT)
    
    # Return the MariaDB datetime string.
    return mariadb_datetime_str


def servicenow_time_worked_to_seconds(time_worked_datetime_str: str) -> int:
    """
    Given a datetime string, convert it to a UTC timestamp, which should
    return how many seconds away from the UTC epoch it is. ServiceNow
    counts up from the epoch to calculate how many seconds someone has
    worked on a ticket for.

    Args:
        time_worked_datetime_str (str): The datetime string to get the
            timestamp for.

    Returns:
        int: The number of seconds away from the UTC epoch (time worked).
    """
    
    # Convert the datetime string into a datetime object.
    time_worked_datetime = datetime.strptime(time_worked_datetime_str, SERVICENOW_DATETIME_FORMAT)
    time_worked_datetime = pytz.utc.localize(time_worked_datetime)
    
    # Extract the timestamp to get how many seconds of time worked there are.
    time_worked_seconds = int(time_worked_datetime.timestamp())
    
    # Return the time worked in seconds.
    return time_worked_seconds


def ticket_dict_to_mariadb_tuple(ticket_dict: dict) -> tuple:
    """
    Converts a ServiceNow ticket dictionary from a response payload into a
    tuple that is formatted to be compatible with MariaDB.

    Args:
        ticket_dict (dict): A dictionary from the pysnow ServiceNow SDK.

    Returns:
        tuple: A tuple of the ServiceNow ticket dictionary that is compatible
            with MariaDB.
    """
    
    # Format and return the dictionary into a tuple that is compatible with
    # MariaDB.
    ticket_tuple = (
        None if ticket_dict.get('assigned_to', None) is None or \
                ticket_dict['assigned_to']['display_value'] == '' \
             else ticket_dict['assigned_to']['display_value'],
        None if ticket_dict.get('assignment_group', None) is None or \
                ticket_dict['assignment_group']['display_value'] == '' \
             else ticket_dict['assignment_group']['display_value'],
        None if ticket_dict.get('caller_id', None) is None or \
                ticket_dict['caller_id']['display_value'] == '' \
             else ticket_dict['caller_id']['display_value'],
        None if ticket_dict.get('cat_item', None) is None or \
                ticket_dict['cat_item']['display_value'] == '' \
             else ticket_dict['cat_item']['display_value'],
        None if ticket_dict.get('category', None) is None or \
                ticket_dict['category']['display_value'] == '' \
             else ticket_dict['category']['display_value'],
        None if ticket_dict.get('closed_at', None) is None \
             else to_mariadb_datetime(ticket_dict['closed_at']['value'], SERVICENOW_DATETIME_FORMAT),
        None if ticket_dict.get('closed_by', None) is None or \
                ticket_dict['closed_by']['display_value'] == '' \
             else ticket_dict['closed_by']['display_value'],
        None if ticket_dict.get('cmdb_ci', None) is None or \
                ticket_dict['cmdb_ci']['display_value'] == '' \
             else ticket_dict['cmdb_ci']['display_value'],
        None if ticket_dict.get('company', None) is None or \
                ticket_dict['company']['display_value'] == '' \
             else ticket_dict['company']['display_value'],
        None if ticket_dict.get('sys_created_on', None) is None \
             else to_mariadb_datetime(ticket_dict['sys_created_on']['value'], SERVICENOW_DATETIME_FORMAT),
        None if ticket_dict.get('location', None) is None or \
                ticket_dict['location']['display_value'] == '' \
             else ticket_dict['location']['display_value'],
        None if ticket_dict.get('u_milestone', None) is None or \
                ticket_dict['u_milestone']['display_value'] == '' \
             else ticket_dict['u_milestone']['display_value'],
        None if ticket_dict.get('number', None) is None or \
                ticket_dict['number']['display_value'] == '' \
             else ticket_dict['number']['display_value'],
        None if ticket_dict.get('opened_by', None) is None or \
                ticket_dict['opened_by']['display_value'] == '' \
             else ticket_dict['opened_by']['display_value'],
        None if ticket_dict.get('priority', None) is None or \
                ticket_dict['priority']['display_value'] == '' \
             else ticket_dict['priority']['display_value'],
        None if ticket_dict.get('request', None) is None or \
                ticket_dict['request']['display_value'] == '' \
             else ticket_dict['request']['display_value'],
        None if ticket_dict.get('requested_for', None) is None or \
                ticket_dict['requested_for']['display_value'] == '' \
             else ticket_dict['requested_for']['display_value'],
        None if ticket_dict.get('risk', None) is None or \
                ticket_dict['risk']['display_value'] == '' \
             else ticket_dict['risk']['display_value'],
        None if ticket_dict.get('severity', None) is None or \
                ticket_dict['severity']['display_value'] == '' \
             else ticket_dict['severity']['display_value'],
        None if ticket_dict.get('short_description', None) is None or \
                ticket_dict['short_description']['display_value'] == '' \
             else ticket_dict['short_description']['display_value'],
        None if ticket_dict.get('u_source_of_level_of_effort', None) is None or \
                ticket_dict['u_source_of_level_of_effort']['display_value'] == '' \
             else ticket_dict['u_source_of_level_of_effort']['display_value'],
        None if ticket_dict.get('state', None) is None or \
                ticket_dict['state']['display_value'] == '' \
             else ticket_dict['state']['display_value'],
        None if ticket_dict.get('subcategory', None) is None or \
                ticket_dict['subcategory']['display_value'] == '' \
             else ticket_dict['subcategory']['display_value'],
        None if ticket_dict.get('sys_class_name', None) is None or \
                ticket_dict['sys_class_name']['value'] == '' \
             else ticket_dict['sys_class_name']['value'],
        None if ticket_dict.get('time_worked', None) is None or \
                ticket_dict['time_worked']['value'] == '' \
             else servicenow_time_worked_to_seconds(ticket_dict['time_worked']['value']),
        None if ticket_dict.get('sys_updated_on', None) is None \
             else to_mariadb_datetime(ticket_dict['sys_updated_on']['value'], SERVICENOW_DATETIME_FORMAT),
        None if ticket_dict.get('sys_updated_by', None) is None or \
                ticket_dict['sys_updated_by']['display_value'] == '' \
             else ticket_dict['sys_updated_by']['display_value'],
        None if ticket_dict.get('urgency', None) is None or \
                ticket_dict['urgency']['display_value'] == '' \
             else ticket_dict['urgency']['display_value'],
        ticket_dict['sys_id']['display_value']
    )
    return ticket_tuple


def get_raw_servicenow_tickets_page(servicenow_table: pysnow.Resource, table_query: pysnow.QueryBuilder | dict | str | None, offset: int) -> list[dict]:
    """
    Gets the raw response from ServiceNow from the provided table, query, and
    offset. Most importantly, we need to check if the query is empty so we can
    call the method with the proper parameters to successfully get a response
    from ServiceNow.

    Args:
        servicenow_table (pysnow.Resource): The table to query in ServiceNow.
        table_query (str): The query to send to ServiceNow.
        offset (int): The offset for the query in case we are calling this
            function multiple times to get all the records.

    Returns:
        list[dict]: The raw response from the ServiceNow results.
    """
    
    # Get a page of results from ServiceNow.
    if table_query is None or table_query == '':
        servicenow_table_tickets_response_page = servicenow_table.get(
            fields=SERVICENOW_TICKET_FIELDS,
            limit=SERVICENOW_MAX_RESPONSE_LIMIT,
            offset=offset
        )
    else:
        servicenow_table_tickets_response_page = servicenow_table.get(
            table_query,
            fields=SERVICENOW_TICKET_FIELDS,
            limit=SERVICENOW_MAX_RESPONSE_LIMIT,
            offset=offset
    )
    
    # Return the page of ServiceNow tickets.
    servicenow_page_records = servicenow_table_tickets_response_page.all()
    return servicenow_page_records


def get_servicenow_tickets(servicenow_table: pysnow.Resource, table_query: pysnow.QueryBuilder | dict | str | None) -> list[tuple]:
    """
    Returns a list of ServiceNow tickets in the form of tuples that are
    compatible with MariaDB from the provided ServiceNow table resource.

    Args:
        servicenow_table (pysnow.Resource): The table to query for tickets.
        table_query (pysnow.QueryBuilder | dict | str | None): The query for
            the table in ServiceNow.

    Returns:
        list[tuple]: The list of MariaDB-compatible tickets in the form of
            tuples.
    """
    
    # Keep track of the offset.
    current_offset = 0
    
    # Get the first page of the ServiceNow table's tickets.
    servicenow_page_records = get_raw_servicenow_tickets_page(servicenow_table, table_query, current_offset)
    
    # As long as there are more pages, convert the tickets into tuples.
    servicenow_ticket_tuples = list[tuple]()
    while len(servicenow_page_records) > 0:
        # For each ticket, convert it into a MariaDB-compatible tuple.
        for raw_servicenow_ticket in servicenow_page_records:
            servicenow_ticket_tuples.append(ticket_dict_to_mariadb_tuple(raw_servicenow_ticket))
        
        # Get the offset for the next page of data.
        current_offset += SERVICENOW_MAX_RESPONSE_LIMIT
        
        # Get the next page in the ServiceNow table's tickets.
        servicenow_page_records = get_raw_servicenow_tickets_page(servicenow_table, table_query, current_offset)
    
    # Return the tickets from this table in ServiceNow as a list of tuples.
    return servicenow_ticket_tuples
    

def insert_servicenow_tickets_into_mariadb(mariadb_connection: MariaDBConnection, servicenow_tickets: list[tuple]) -> int:
    """
    Inserts the provided ServiceNow tickets into MariaDB using the provided 
    MariaDB connection. This function will send the tickets in batches to avoid
    insertion errors. It returns the number of rows inserted into MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
        servicenow_tickets (list[tuple]): The ServiceNow tickets to send to
            MariaDB.

    Returns:
        int: The total number of rows inserted.
    """
    
    # Initialize reference variables.
    mariadb_cursor = mariadb_connection.cursor()
    total_rows_inserted = 0
    
    # Push the ServiceNow tickets in batches to MariaDB.
    for servicenow_ticket_batch in batched(servicenow_tickets, MARIADB_EXECUTE_MANY_LIMIT):
        # Insert the batch into MariaDB.
        logger.info('Sending a batch of ServiceNow tickets to MariaDB...')
        try:
            mariadb_cursor.executemany(
                f'INSERT IGNORE INTO {MARIADB_SERVICENOW_TABLE_NAME} ' \
                f'VALUES ({MARIADB_SERVICENOW_INSERT_PLACEHOLDERS})',
                servicenow_ticket_batch
            )
            mariadb_connection.commit()
            total_rows_inserted += mariadb_cursor.rowcount
        except mariadb.Error as e:
            logger.error(f'{e}')
            logger.warning(
                'Trying this batch again, but 1 ticket at a time. ' \
                'The erroring record will be printed to the console and '
                'batching will resume as normal after this batch.'
            )
                
            # For each ServiceNow ticket in this batch, send the insert command to MariaDB one at a time.
            for servicenow_ticket in servicenow_ticket_batch:
                # Insert the ticket into MariaDB.
                try:
                    mariadb_cursor.execute(
                        f'INSERT IGNORE INTO {MARIADB_SERVICENOW_TABLE_NAME} ' \
                        f'VALUES ({MARIADB_SERVICENOW_INSERT_PLACEHOLDERS})',
                        servicenow_ticket
                    )
                    mariadb_connection.commit()
                    total_rows_inserted += mariadb_cursor.rowcount
                except mariadb.Error as e:
                    logger.error(f'{e}')
                    logger.error(f'Object: {servicenow_ticket}')
        
        logger.info(f'Inserted {len(servicenow_ticket_batch)} ServiceNow tickets into MariaDB!')
        
    # Return how many rows were inserted.
    return total_rows_inserted


def mariadb_servicenow_tickets_table_seeding(mariadb_connection: MariaDBConnection) -> None:
    """
    Seeds the provided table via the provided MariaDB connection with every
    single supported ServiceNow ticket.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
    """
    
    logger.info(f'Seeding the ServiceNow table in MariaDB with all ServiceNow tickets...')

    # Make the list that will hold all the tickets in ServiceNow.
    all_servicenow_tickets = list[tuple]()

    # Get all incident tickets from ServiceNow and add it to the tickets list.
    logger.info('Getting all incident tickets from ServiceNow...')
    servicenow_incident_table = SERVICENOW_CLIENT.resource(api_path='/table/incident')
    servicenow_incident_tickets = get_servicenow_tickets(servicenow_incident_table, None)
    all_servicenow_tickets.extend(servicenow_incident_tickets)
    logger.info(f'Recieved {len(servicenow_incident_tickets)} incident tickets from ServiceNow!')
    
    # Get all request item tickets from ServiceNow and add it to the tickets list.
    logger.info('Getting all request item tickets from ServiceNow...')
    servicenow_request_item_table = SERVICENOW_CLIENT.resource(api_path='/table/sc_req_item')
    servicenow_request_item_tickets = get_servicenow_tickets(servicenow_request_item_table, None)
    all_servicenow_tickets.extend(servicenow_request_item_tickets)
    logger.info(f'Recieved {len(servicenow_request_item_tickets)} request item tickets from ServiceNow!')
    
    # Get all change request tickets from ServiceNow and add it to the tickets list.
    logger.info('Getting all change request tickets from ServiceNow...')
    servicenow_change_request_table = SERVICENOW_CLIENT.resource(api_path='/table/change_request')
    servicenow_change_request_tickets = get_servicenow_tickets(servicenow_change_request_table, None)
    all_servicenow_tickets.extend(servicenow_change_request_tickets)
    logger.info(f'Recieved {len(servicenow_change_request_tickets)} change request tickets from ServiceNow!')
    
    # Insert all the ServiceNow tickets into MariaDB.
    total_inserts = insert_servicenow_tickets_into_mariadb(mariadb_connection, all_servicenow_tickets)
    logger.info(f'Inserted {total_inserts} ServiceNow tickets into MariaDB!')
    
    logger.info(f'ServiceNow table in MariaDB has been seeded with all ServiceNow tickets!')


def get_opsgenie_alert_as_tuple(opsgenie_alert_id: str) -> tuple | None:
    """
    Given an id of an Opsgenie alert, get the alert data from the Opsgenie API 
    and return it as a MariaDB-compatible tuple.

    Args:
        opsgenie_alert_id (str): The id of the Opsgenie alert.

    Returns:
        tuple | None: The tuple of the alert data from Opsgenie or None if the
            alert was not found.
    """
    
    # Send the request to the Opsgenie API and convert the response to JSON.
    opsgenie_get_alert_raw_response = requests.get(
        url=f'{OPSGENIE_BASE_API_URL}/alerts/{opsgenie_alert_id}',
        headers={
            'Authorization': f'GenieKey {OPSGENIE_API_KEY}'                                
        }
    )
    opsgenie_get_alert_response = opsgenie_get_alert_raw_response.json()
    
    # Check if the alert data is None. Return None if so.
    if opsgenie_get_alert_response.get('data', None) is None:
        return None

    # Convert and return the alert as a MariaDB-compatible tuple.
    return alert_dict_to_mariadb_tuple(opsgenie_get_alert_response['data'])


def get_needed_opsgenie_updates_for_mariadb(mariadb_connection: MariaDBConnection) -> list[tuple]:
    """
    Gets all Opsgenie alerts that are not closed in MariaDB and requests the
    latest update for each one from the Opsgenie API. Returns a list of those
    updates in the form of tuples.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.

    Returns:
        list[tuple]: The list of the latest Opsgenie alert updates.
    """
    
    # Get all alerts from MariaDB that are not closed so we can update them.
    total_deletions = 0
    mariadb_cursor = mariadb_connection.cursor()
    mariadb_cursor.execute(
        f'SELECT id FROM {MARIADB_OPSGENIE_TABLE_NAME} ' \
        f'WHERE status <> "closed"'
    )
    
    # Check if there are no alerts to update.
    number_of_updates = mariadb_cursor.rowcount
    if number_of_updates == 0:
        return []
    
    # Extract each Opsgenie alert's ID that needs to be updated.
    opsgenie_alert_ids_to_update = list()
    for (id,) in mariadb_cursor:
        opsgenie_alert_ids_to_update.append(id)
    
    # Get each Opsgenie alert's update from the Opsgenie API.
    opsgenie_alert_updates = list[tuple]()
    for opsgenie_alert_id in opsgenie_alert_ids_to_update:
        # Send the request to the Opsgenie API and convert the response to JSON.
        opsgenie_get_alert_raw_response = requests.get(
            url=f'{OPSGENIE_BASE_API_URL}/alerts/{opsgenie_alert_id}',
            headers={
                'Authorization': f'GenieKey {OPSGENIE_API_KEY}'                                
            }
        )
        opsgenie_get_alert_response = opsgenie_get_alert_raw_response.json()
        
        # Check if the alert data is None. This means the alert was deleted in Opsgenie,
        # so we must reflect that in MariaDB by deleting it from MariaDB.
        if opsgenie_get_alert_response.get('data', None) is None:
            logger.warning(f'Deleting record with id "{opsgenie_alert_id}" from MariaDB as it was deleted in Opsgenie.')
            try:
                mariadb_cursor.execute(
                    f"DELETE FROM {MARIADB_OPSGENIE_TABLE_NAME} " \
                    f"WHERE id = '{opsgenie_alert_id}'"
                )
                mariadb_connection.commit()
                total_deletions += 1
            except mariadb.Error as e:
                logger.error(f'{e}')
                logger.error(f'id: {opsgenie_alert_id}')
            
            # Don't add this tuple to the alert updates since we just deleted it from MariaDB.
            logger.info('Record deleted from MariaDB!')
            continue
        
        # Convert and put the update into the list of alert updates.
        opsgenie_raw_alert = opsgenie_get_alert_response['data']
        alert_tuple = alert_dict_to_mariadb_tuple(opsgenie_raw_alert)
        opsgenie_alert_updates.append(alert_tuple)
        
    logger.info(f'Deleted {total_deletions} Opsgenie alerts from MariaDB!')
    return opsgenie_alert_updates


def update_opsgenie_alerts_in_mariadb(mariadb_connection: MariaDBConnection, opsgenie_alert_updates: list[tuple]) -> int:
    """
    Updates MariaDB with the provided Opsgenie alerts. Returns how many rows
    were updated in MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
        opsgenie_alert_updates (list[tuple]): The Opsgenie alerts to update in
            MariaDB.

    Returns:
        int: The number of rows that were updated in MariaDB.
    """
    
    # Push this page of Opsgenie alerts as batches into MariaDB.
    total_updates = 0
    mariadb_cursor = mariadb_connection.cursor()
    for opsgenie_alert_updates_batch in batched(opsgenie_alert_updates, MARIADB_EXECUTE_MANY_LIMIT):
        try:
            mariadb_cursor.executemany(
                f"UPDATE {MARIADB_OPSGENIE_TABLE_NAME} " \
                f"SET {MARIADB_OPSGENIE_UPDATE_PLACEHOLDERS} = ? " \
                f"WHERE id = ?",
                opsgenie_alert_updates_batch
            )
            mariadb_connection.commit()
            total_updates += mariadb_cursor.rowcount
        except mariadb.Error as e:
            logger.error(f'{e}')
            logger.warning(
                'Trying this batch again, but 1 alert at a time. The erroring '
                'record will be printed to the console and '
                'batching will resume as normal after this batch.'
            )
            
            # For each Opsgenie alert in this batch, send the update command to MariaDB one alert at a time.
            for opsgenie_alert_update in opsgenie_alert_updates_batch:
                # Update the alert into MariaDB.
                try:
                    mariadb_cursor.execute(
                        f"UPDATE {MARIADB_OPSGENIE_TABLE_NAME} " \
                        f"SET {MARIADB_OPSGENIE_UPDATE_PLACEHOLDERS} = ? " \
                        f"WHERE id = ?",
                        opsgenie_alert_update
                    )
                    mariadb_connection.commit()
                    total_updates += mariadb_cursor.rowcount
                except mariadb.Error as e:
                    logger.error(f'{e}')
                    logger.error(f'Object: {opsgenie_alert_update}')
    
    return total_updates


def update_mariadb_opsgenie_alerts(mariadb_connection: MariaDBConnection) -> None:
    """
    Looks for any Opsgenie alerts in MariaDB that are not in the "closed" state
    and will grab their current status from the Opsgenie API. It will then
    update the associated records in MariaDB with the latest updates in case
    they were closed out or are in a different state.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
    """
    
    logger.info('Updating all Opsgenie alerts that are not closed...')

    # Use the Opsgenie API to get a list of the latest updates of alerts in 
    # MariaDB that need to be updated.
    opsgenie_alert_updates = get_needed_opsgenie_updates_for_mariadb(mariadb_connection)
    
    # Update the Opsgenie alerts in MariaDB that need to be updated.
    total_updates = update_opsgenie_alerts_in_mariadb(mariadb_connection, opsgenie_alert_updates)
    
    logger.info(f'Updated {total_updates} Opsgenie alerts in MariaDB!')
    logger.info('All Opsgenie alerts have been updated in MariaDB!')


def get_all_latest_opsgenie_alerts(mariadb_connection: MariaDBConnection) -> list[tuple]:
    """
    Returns all Opsgenie alerts that have been generated since the last time 
    this script ran. It will return the result in the form of a list of tuples.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.

    Returns:
        list[tuple]: The Opsgenie alerts that are missing from MariaDB.
    """
    
    # Get the latest alert record from the MariaDB.
    mariadb_cursor = mariadb_connection.cursor()
    mariadb_cursor.execute(
        f'SELECT created_at FROM {MARIADB_OPSGENIE_TABLE_NAME} ' \
        f'ORDER BY created_at ' \
        f'DESC'
    )
    
    # Extract the latest alert's created_at datetime.
    latest_alert_datetime = mariadb_cursor.fetchone()[0]
    
    # Extract the timestamp from the alert.
    latest_alert_timestamp = int(latest_alert_datetime.replace(tzinfo=timezone('UTC')).timestamp() * 1000)
    
    # Get the first batch of Opsgenie alerts.
    opsgenie_alerts_query = f'createdAt > {latest_alert_timestamp}'
    opsgenie_alerts = get_opsgenie_alerts(opsgenie_alerts_query)
    latest_opsgenie_alerts = list[tuple]()
    
    # As long as there are more pages of Opsgenie alerts, keep going.
    while len(opsgenie_alerts) > 0:
        # Add the alerts to the return list.
        latest_opsgenie_alerts.extend(opsgenie_alerts)
        
        # Parse the most recent alert creation datetime as a datetime object.
        most_recent_alert_datetime = datetime.strptime(opsgenie_alerts[0][OPSGENIE_ALERT_CREATED_AT_INDEX], MARIADB_DATETIME_FORMAT)
        
        # Make the datetime object timezone aware.
        most_recent_alert_datetime_utc = most_recent_alert_datetime.replace(tzinfo=timezone('UTC'))
        
        # Convert the datetime object into a UTC timestamp.
        most_recent_alert_timestamp = int(most_recent_alert_datetime_utc.timestamp() * 1000)
        opsgenie_alerts_query = f'createdAt > {most_recent_alert_timestamp}'
        
        # Get the next batch of Opsgenie alerts.
        opsgenie_alerts = get_opsgenie_alerts(opsgenie_alerts_query)
    
    # Return all the latest Opsgenie alerts.
    return latest_opsgenie_alerts


def push_latest_opsgenie_alerts_to_mariadb(mariadb_connection: MariaDBConnection) -> None:
    """
    Retrieves and inserts all missing Opsgenie alerts since the creation of the
    latest alert in MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
    """

    logger.info('Getting all the latest Opsgenie alerts...')
    
    # Get all the latest alerts starting after the created_at timestamp.
    latest_opsgenie_alerts = get_all_latest_opsgenie_alerts(mariadb_connection)
    
    # Insert all the latest alerts into the MariaDB.
    total_inserts = insert_opsgenie_alerts_into_mariadb(mariadb_connection, latest_opsgenie_alerts)
    logger.info(f'Inserted {total_inserts} Opsgenie alerts into MariaDB!')
    
    logger.info('Pushed all the latest Opsgenie alerts!')


def get_servicenow_ticket_as_tuple(sys_id: str, ticket_type_table: str) -> tuple | None:
    """
    Given a sys_id of a ServiceNow ticket and its associated table name, get
    the ticket data from ServiceNow and return it as a MariaDB-compatible
    tuple.

    Args:
        sys_id (str): The sys_id of the ServiceNow ticket.
        ticket_type_table (str): The table name the ticket is associated with.

    Returns:
        tuple | None: The tuple of the ticket data from ServiceNow or None if
            the ticket was not found.
    """
    
    # Get the relevant table and query it for the ticket.
    servicenow_table = SERVICENOW_CLIENT.resource(api_path=f'/table/{ticket_type_table}')
    servicenow_ticket = get_servicenow_tickets(servicenow_table, f'sys_id={sys_id}')
    
    # Return the ticket as a tuple or None if the ticket doesn't exist.
    return None if len(servicenow_ticket) == 0 else servicenow_ticket[0]
    

def get_needed_servicenow_updates_for_mariadb(mariadb_connection: MariaDBConnection) -> list[tuple]:
    """
    Gets all ServiceNow tickets that are not closed in MariaDB and requests the
    latest update for each one from ServiceNow. Returns a list of those updates
    in the form of tuples.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.

    Returns:
        list[tuple]: The list of the latest ServiceNow ticket updates.
    """
    
    # Get all tickets from MariaDB that are not closed so we can update them.
    total_deletions = 0
    mariadb_cursor = mariadb_connection.cursor()
    mariadb_cursor.execute(
        f'SELECT sys_id,ticket_type_table FROM {MARIADB_SERVICENOW_TABLE_NAME} ' \
        f'WHERE state <> "Cancel - Duplicate" AND ' \
        f'state <> "Canceled" AND state <> "Closed" AND state <> "Closed Complete" AND ' \
        f'state <> "Closed Incomplete" AND state <> "Closed Skipped"'
    )
    
    # Check if there are no tickets to update.
    number_of_updates = mariadb_cursor.rowcount
    if number_of_updates == 0:
        return []
    
    # Extract each ServiceNow ticket's sys_id and the table the ticket comes
    # from that needs to be updated.
    servicenow_tickets_to_update = list[tuple]()
    for (sys_id, ticket_type_table) in mariadb_cursor:
        servicenow_tickets_to_update.append((sys_id, ticket_type_table))
    
    # Get each ServiceNow ticket's update from the ServiceNow CMDB.
    logger.info('Fetching update data from ServiceNow...')
    servicenow_ticket_updates = list[tuple]()
    for (servicenow_ticket_id, servicenow_ticket_type_table) in servicenow_tickets_to_update:
        # Convert the raw ticket as a tuple and add it to the list of tickets to update.
        ticket_tuple = get_servicenow_ticket_as_tuple(servicenow_ticket_id, servicenow_ticket_type_table)
        
        # Check if the ticket tuple is None. This means the ticket was deleted in ServiceNow,
        # so we must reflect that in MariaDB by deleting it from MariaDB.
        if ticket_tuple is None:
            logger.warning(f'Deleting record with sys_id "{servicenow_ticket_id}" from MariaDB as it was deleted in ServiceNow.')
            try:
                mariadb_cursor.execute(
                    f"DELETE FROM {MARIADB_SERVICENOW_TABLE_NAME} " \
                    f"WHERE sys_id = '{servicenow_ticket_id}'"
                )
                mariadb_connection.commit()
                total_deletions += 1
            except mariadb.Error as e:
                logger.error(f'{e}')
                logger.error(f'sys_id: {servicenow_ticket_id}')
            
            # Don't add this tuple to the ticket updates since we just deleted it from MariaDB.
            logger.info('Record deleted from MariaDB!')
            continue
        
        servicenow_ticket_updates.append(ticket_tuple)
    
    logger.info(f'Deleted {total_deletions} ServiceNow tickets from MariaDB!')
    return servicenow_ticket_updates


def update_servicenow_tickets_in_mariadb(mariadb_connection: MariaDBConnection, servicenow_ticket_updates: list[tuple]) -> int:
    """
    Updates MariaDB with the provided ServiceNow tickets. Returns how many rows
    were updated in MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
        opsgenie_alert_updates (list[tuple]): The ServiceNow tickets to update
            in MariaDB.

    Returns:
        int: The number of rows that were updated in MariaDB.
    """
    
    # Push this page of ServiceNow tickets as batches into MariaDB.
    total_updates = 0
    mariadb_cursor = mariadb_connection.cursor()
    for servicenow_ticket_updates_batch in batched(servicenow_ticket_updates, MARIADB_EXECUTE_MANY_LIMIT):
        try:
            mariadb_cursor.executemany(
                f"UPDATE {MARIADB_SERVICENOW_TABLE_NAME} " \
                f"SET {MARIADB_SERVICENOW_UPDATE_PLACEHOLDERS} = ? " \
                f"WHERE sys_id = ?",
                servicenow_ticket_updates_batch
            )
            mariadb_connection.commit()
            total_updates += mariadb_cursor.rowcount
        except mariadb.Error as e:
            logger.error(f'{e}')
            logger.warning(
                'Trying this batch again, but 1 alert at a time. '
                'The erroring record will be printed to the console and '
                'batching will resume as normal after this batch.'
            )
            
            # For each ServiceNow ticket in this batch, send the update command to MariaDB one ticket at a time.
            for servicenow_ticket_update in servicenow_ticket_updates_batch:
                # Insert the ticket into MariaDB.
                try:
                    mariadb_cursor.execute(
                        f"UPDATE {MARIADB_SERVICENOW_TABLE_NAME} " \
                        f"SET {MARIADB_SERVICENOW_UPDATE_PLACEHOLDERS} = ? " \
                        f"WHERE sys_id = ?",
                        servicenow_ticket_update
                    )
                    mariadb_connection.commit()
                    total_updates += mariadb_cursor.rowcount
                except mariadb.Error as e:
                    logger.error(f'{e}')
                    logger.error(f'Object: {servicenow_ticket_update}')
    
    return total_updates


def update_mariadb_servicenow_tickets(mariadb_connection: MariaDBConnection) -> None:
    """
    Looks for any ServiceNow tickets in MariaDB that are not in a closed-type
    state and will grab their current status from ServiceNow. It will then
    update the associated records in MariaDB with the latest updates in case
    they were closed out or are in a different state.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
    """
    
    logger.info('Updating all ServiceNow tickets that are not closed in MariaDB...')
    servicenow_ticket_updates = get_needed_servicenow_updates_for_mariadb(mariadb_connection)
    
    logger.info('Update data has been fetched from ServiceNow!')
    logger.info('Pushing update data to MariaDB...')
    
    total_updates = update_servicenow_tickets_in_mariadb(mariadb_connection, servicenow_ticket_updates)
    
    logger.info(f'Updated {total_updates} ServiceNow tickets in MariaDB!')
    logger.info('All ServiceNow tickets have been updated in MariaDB!')


def get_all_latest_servicenow_tickets(mariadb_connection: MariaDBConnection) -> list[tuple]:
    """
    Using the provided connection to MariaDB, this function will get all 
    ServiceNow tickets that are missing in MariaDB since the last time this
    script ran. It will return the result in the form of a list of tuples.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.

    Returns:
        list[tuple]: The ServiceNow tickets missing from MariaDB.
    """
    
    logger.info('Getting all latest ServiceNow tickets...')
    
    # Make the list to return.
    latest_servicenow_tickets = list[tuple]()
    
    # Get all the latest incident tickets from ServiceNow.
    latest_incident_tickets = get_all_latest_servicenow_tickets_of_type(mariadb_connection, 'incident')
    latest_servicenow_tickets.extend(latest_incident_tickets)
    
    # Get all the latest request item tickets from ServiceNow.
    latest_request_item_tickets = get_all_latest_servicenow_tickets_of_type(mariadb_connection, 'sc_req_item')
    latest_servicenow_tickets.extend(latest_request_item_tickets)
    
    # Get all the latest change request tickets from ServiceNow.
    latest_change_request_tickets = get_all_latest_servicenow_tickets_of_type(mariadb_connection, 'change_request')
    latest_servicenow_tickets.extend(latest_change_request_tickets)
    
    # Return all the latest ServiceNow tickets.
    logger.info('Received all latest ServiceNow tickets!')
    return latest_servicenow_tickets


def get_latest_servicenow_ticket_type_datetime_from_mariadb(mariadb_connection: MariaDBConnection, servicenow_ticket_type: str) -> datetime:
    """
    Returns the datetime of the latest ServiceNow ticket of the provided ticket
    type inside MariaDB. Since there can be multiple ticket types at a time in
    the MariaDB ServiceNow table, we need to be able to distinguish the ticket
    type.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
        servicenow_ticket_type (str): The ticket type to get the latest
            ticket creation for.

    Returns:
        datetime: The datetime of the latest ticket of the provided type.
    """
    
    # Make a reference to the MariaDB cursor.
    mariadb_cursor = mariadb_connection.cursor()
    
    # Get the latest datetime for this ticket type from MariaDB.
    mariadb_cursor.execute(
        f"SELECT created_at FROM {MARIADB_SERVICENOW_TABLE_NAME} " \
        f"WHERE ticket_type_table = '{servicenow_ticket_type}' " \
        f"ORDER BY created_at " \
        f"DESC"
    )
    
    # Extract the datetime from the result and return it.
    mariadb_response = mariadb_cursor.fetchone()
    latest_ticket_creation_datetime = mariadb_response[0]
    
    return latest_ticket_creation_datetime


def get_all_latest_servicenow_tickets_of_type(mariadb_connection: MariaDBConnection, servicenow_ticket_type: str) -> list[tuple]:
    """
    Gets all the latest ServiceNow tickets of the specified type from
    ServiceNow and returns it as a list of MariaDB-compatible tuples. It uses
    the MariaDB connection to get the datetime of the latest record in the 
    ServiceNow ticket table in MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
        servicenow_ticket_type (str): The type of tickets to get from
            ServiceNow.

    Returns:
        list[tuple]: The list of tickets as MariaDB-compatible tuples.
    """
    
    # Get the latest datetime from MariaDB for the ticket type.
    latest_ticket_datetime = get_latest_servicenow_ticket_type_datetime_from_mariadb(mariadb_connection, servicenow_ticket_type)
    
    # Extract the ticket's created_at datetime and convert it to the US/Eastern timezone.
    latest_ticket_datetime_utc = pytz.utc.localize(latest_ticket_datetime)
    latest_ticket_datetime_est = latest_ticket_datetime_utc.astimezone(timezone('US/Eastern'))
    latest_ticket_datetime_est = timezone('US/Eastern').normalize(latest_ticket_datetime_est)
    
    # Get a reference to the ServiceNow table and make the query to the table.
    servicenow_table = SERVICENOW_CLIENT.resource(api_path=f'/table/{servicenow_ticket_type}')
    servicenow_tickets_query = f"sys_created_on>javascript:gs.dateGenerate('{latest_ticket_datetime_est.strftime("%Y-%m-%d")}','{latest_ticket_datetime_est.strftime("%H:%M:%S")}')"
    
    # Get the latest tickets from this ticket table from ServiceNow and return them.
    latest_tickets = get_servicenow_tickets(servicenow_table, servicenow_tickets_query)
    return latest_tickets


def push_latest_servicenow_tickets_to_mariadb(mariadb_connection: MariaDBConnection) -> None:
    """
    Retrieves and inserts all missing ServiceNow tickets since the creation of
    the latest ticket in MariaDB.

    Args:
        mariadb_connection (MariaDBConnection): The connection to MariaDB.
    """
    
    logger.info('Pushing all the latest ServiceNow tickets into MariaDB...')
    
    # Get all the latest ServiceNow tickets missing in MariaDB.
    latest_servicenow_tickets = get_all_latest_servicenow_tickets(mariadb_connection)
    
    # Insert all the latest tickets into MariaDB.
    total_inserts = insert_servicenow_tickets_into_mariadb(mariadb_connection, latest_servicenow_tickets)
    logger.info(f'Inserted {total_inserts} ServiceNow tickets into MariaDB!')
    
    logger.info('Pushed all the latest ServiceNow tickets into MariaDB!')


def customer_data_to_mariadb() -> None:
    """
    Sychronizes the latest Opsgenie alerts and ServiceNow tickets to MariaDB.
    """
    
    logger.info('Beginning customer data sync to MariaDB...')
    
    # Establish the connection to MariaDB.
    mariadb_connection = mariadb.connect(
        user=MARIADB_USERNAME,
        password=MARIADB_PASSWORD,
        host=MARIADB_IP_ADDRESS,
        database=MARIADB_DATABASE_NAME
    )
    mariadb_cursor = mariadb_connection.cursor()
    
    # Get the number of records in the Opsgenie table in MariaDB.
    mariadb_cursor.execute(f'SELECT COUNT(*) FROM {MARIADB_OPSGENIE_TABLE_NAME}')
    opsgenie_alerts_table_row_count = mariadb_cursor.fetchone()[0]
    
    # Check if the Opsgenie Alerts table is empty. If so, seed it. If not,
    # update non-closed alerts and insert the latest alerts.
    if opsgenie_alerts_table_row_count == 0:
        logger.info('The Opsgenie table in MariaDB is empty, so we must seed it.')
        mariadb_opsgenie_alerts_table_seeding(mariadb_connection)
    else:
        # Update any existing Opsgenie records in MariaDB that need to be updated.
        update_mariadb_opsgenie_alerts(mariadb_connection)
        
        # Push all new Opsgenie alerts since the latest alert record in MariaDB.
        push_latest_opsgenie_alerts_to_mariadb(mariadb_connection)
    
    # Get the number of records in the ServiceNow table in MariaDB.
    mariadb_cursor.execute(f'SELECT COUNT(*) FROM {MARIADB_SERVICENOW_TABLE_NAME}')
    servicenow_tickets_table_row_count = mariadb_cursor.fetchone()[0]
    
    # Check if the ServiceNow tickets table is empty. If so, seed it. If not,
    # update non-closed tickets and insert the latest tickets.
    if servicenow_tickets_table_row_count == 0:
        logger.info('The ServiceNow table in MariaDB is empty, so we must seed it.')
        mariadb_servicenow_tickets_table_seeding(mariadb_connection)
    else:
        # Update any existing ServiceNow records in MariaDB that need to be updated.
        update_mariadb_servicenow_tickets(mariadb_connection)
        
        # Push all new ServiceNow tickets since the latest ticket record in MariaDB.
        push_latest_servicenow_tickets_to_mariadb(mariadb_connection)
    
    # Close the connection to MariaDB.
    mariadb_connection.close()
    
    logger.info('Customer data sync to MariaDB has completed!')


if __name__ == '__main__':
    customer_data_to_mariadb()
