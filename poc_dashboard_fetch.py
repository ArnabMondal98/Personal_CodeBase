# ========================================
# FILE 1: DB_Fetch_POC_Dashboard.py
# ========================================

import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
import json
import re
from typing import Dict, List, Tuple, Optional
import sys

# Import your existing modules
from DashboardUtils import DashboardUtils
import db_config
import constants


class POCDashboardDataFetcher:
    """
    Fetches POC Dashboard data from Rally API and processes it for database insertion
    Includes spillover calculation, task counting, and data extraction
    """
    
    def __init__(self, api_key: str, cio_info: dict):
        self.api_key = api_key
        self.cio_info = cio_info
        self.base_url = "https://rally1.rallydev.com/slm/webservice/v2.0"
        self.headers = DashboardUtils.get_headers(api_key)
        self.iteration_days = 15  # Default iteration length
        
    def fetch_user_story_details(self, portfolio: str, project_name: str) -> pd.DataFrame:
        """
        Fetch user story details from Rally for POC Dashboard
        
        Args:
            portfolio: Portfolio name
            project_name: Project name
            
        Returns:
            DataFrame with user story data
        """
        try:
            # Construct Rally API query for user stories
            query_params = DashboardUtils.get_query_parameters(constants.POC_DASHBOARD_COLUMN_LIST)
            query_params['query'] = f'(Project.Name = "{project_name}")'
            
            api_url = DashboardUtils.get_rally_api_url("HierarchicalRequirement")
            
            print(f"Fetching POC Dashboard data for project: {project_name}")
            
            response = requests.get(
                api_url,
                headers=self.headers,
                params=query_params,
                timeout=120
            )
            
            if response.status_code == 200:
                results = response.json().get('QueryResult', {}).get('Results', [])
                
                if len(results) > 0:
                    df = pd.DataFrame(results)
                    
                    # Process and add custom columns
                    df = self._process_poc_data(df, portfolio, project_name)
                    
                    return df
                else:
                    print(f"No user stories found for project: {project_name}")
                    return pd.DataFrame()
            else:
                print(f"Error fetching data: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Exception in fetch_user_story_details: {e}")
            return pd.DataFrame()
    
    def _process_poc_data(self, df: pd.DataFrame, portfolio: str, project_name: str) -> pd.DataFrame:
        """
        Process raw Rally data and add custom calculated columns
        
        Args:
            df: Raw dataframe from Rally
            portfolio: Portfolio name
            project_name: Project name
            
        Returns:
            Processed dataframe with additional columns
        """
        processed_data = []
        
        for idx, item in df.iterrows():
            try:
                # Extract basic information
                formatted_id = item.get('FormattedID', '')
                name = item.get('Name', '')
                creation_date = self._parse_date(item.get('CreationDate'))
                
                # Get iteration information
                iteration = item.get('Iteration', {})
                iteration_start = self._parse_date(iteration.get('StartDate')) if iteration else None
                iteration_end = self._parse_date(iteration.get('EndDate')) if iteration else None
                
                # Calculate spillover metrics
                spillover_count, spillover_flag = self._calculate_spillover(
                    creation_date, 
                    item, 
                    iteration_start, 
                    iteration_end
                )
                
                # Get task count
                task_count = self._get_task_count(item)
                
                # Extract description details (first 500 characters)
                description = item.get('Description', '') or ''
                details = self._clean_html_text(description)[:500]
                
                # Extract notes
                notes = self._extract_notes(item)
                
                # Get completion date
                completion_date = self._get_completion_date(item)
                
                # Build processed record
                record = {
                    'Portfolio': portfolio,
                    'ProjectName': project_name,
                    'FormattedID': formatted_id,
                    'Name': name,
                    'CreationDate': creation_date,
                    'State': item.get('ScheduleState', ''),
                    'Owner': self._get_owner_name(item),
                    'PlanEstimate': item.get('PlanEstimate', 0),
                    'Iteration': iteration.get('Name', '') if iteration else '',
                    'IterationStartDate': iteration_start,
                    'IterationEndDate': iteration_end,
                    'SpilloverCount': spillover_count,
                    'SpilloverFlag': spillover_flag,
                    'TaskCount': task_count,
                    'Details': details,
                    'Notes': notes,
                    'CompletionDate': completion_date,
                    'Priority': item.get('Priority', ''),
                    'Blocked': item.get('Blocked', False),
                    'BlockedReason': item.get('BlockedReason', ''),
                    'Tags': self._get_tags(item),
                    'LastUpdateDate': self._parse_date(item.get('LastUpdateDate')),
                    'Release': self._get_release_name(item),
                    'Parent': self._get_parent_name(item),
                }
                
                processed_data.append(record)
                
            except Exception as e:
                print(f"Error processing item {item.get('FormattedID', 'Unknown')}: {e}")
                continue
        
        return pd.DataFrame(processed_data)
    
    def _calculate_spillover(
        self, 
        creation_date: Optional[datetime], 
        item: dict,
        iteration_start: Optional[datetime],
        iteration_end: Optional[datetime]
    ) -> Tuple[int, str]:
        """
        Calculate spillover count and flag based on Rally logic
        
        Logic: If (completion_date - creation_date of first task) > iteration_days, 
               then spillover occurs
        
        Args:
            creation_date: Story creation date
            item: Rally item data
            iteration_start: Iteration start date
            iteration_end: Iteration end date
            
        Returns:
            Tuple of (spillover_count, spillover_flag)
        """
        try:
            # Get first task creation date
            first_task_date = self._get_first_task_creation_date(item)
            
            # Get completion date (when story reached Accepted/Completed state)
            completion_date = self._get_completion_date(item)
            
            if not first_task_date or not completion_date:
                return 0, 'No'
            
            # Calculate working days between first task and completion
            days_to_complete = (completion_date - first_task_date).days
            
            # Check if it exceeds iteration length (default 15 days)
            if iteration_end and iteration_start:
                iteration_length = (iteration_end - iteration_start).days
            else:
                iteration_length = self.iteration_days
            
            if days_to_complete > iteration_length:
                # Calculate how many iterations it spilled over
                spillover_count = (days_to_complete // iteration_length)
                spillover_flag = 'Yes'
            else:
                spillover_count = 0
                spillover_flag = 'No'
            
            return spillover_count, spillover_flag
            
        except Exception as e:
            print(f"Error calculating spillover: {e}")
            return 0, 'No'
    
    def _get_first_task_creation_date(self, item: dict) -> Optional[datetime]:
        """
        Get the creation date of the first task for a user story
        
        Args:
            item: Rally user story item
            
        Returns:
            DateTime of first task creation or None
        """
        try:
            # Fetch tasks for this user story
            tasks_ref = item.get('Tasks', {})
            if not tasks_ref:
                return None
            
            tasks_url = tasks_ref.get('_ref')
            if not tasks_url:
                return None
            
            response = requests.get(
                tasks_url,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                tasks = response.json().get('QueryResult', {}).get('Results', [])
                
                if len(tasks) > 0:
                    # Find earliest task creation date
                    task_dates = [self._parse_date(t.get('CreationDate')) for t in tasks]
                    task_dates = [d for d in task_dates if d is not None]
                    
                    if task_dates:
                        return min(task_dates)
            
            return None
            
        except Exception as e:
            print(f"Error fetching first task date: {e}")
            return None
    
    def _get_task_count(self, item: dict) -> int:
        """
        Get the count of tasks for a user story
        
        Args:
            item: Rally user story item
            
        Returns:
            Number of tasks
        """
        try:
            tasks_ref = item.get('Tasks', {})
            task_count = tasks_ref.get('Count', 0)
            return task_count
            
        except Exception as e:
            print(f"Error getting task count: {e}")
            return 0
    
    def _get_completion_date(self, item: dict) -> Optional[datetime]:
        """
        Get the completion date when story reached Accepted state
        
        Args:
            item: Rally user story item
            
        Returns:
            DateTime of completion or None
        """
        try:
            # Check current state
            schedule_state = item.get('ScheduleState', '')
            
            if schedule_state in ['Accepted', 'Completed']:
                # Try to get AcceptedDate first
                accepted_date = item.get('AcceptedDate')
                if accepted_date:
                    return self._parse_date(accepted_date)
                
                # Fallback to InProgressDate + estimate
                in_progress_date = item.get('InProgressDate')
                if in_progress_date:
                    return self._parse_date(in_progress_date)
            
            return None
            
        except Exception as e:
            print(f"Error getting completion date: {e}")
            return None
    
    def _extract_notes(self, item: dict) -> str:
        """
        Extract notes section from Rally (text only)
        
        Args:
            item: Rally user story item
            
        Returns:
            Cleaned notes text
        """
        try:
            notes = item.get('Notes', '') or ''
            # Clean HTML and get plain text
            cleaned_notes = self._clean_html_text(notes)
            return cleaned_notes[:1000]  # Limit to 1000 characters
            
        except Exception as e:
            print(f"Error extracting notes: {e}")
            return ''
    
    def _clean_html_text(self, html_text: str) -> str:
        """
        Remove HTML tags and get plain text
        
        Args:
            html_text: HTML formatted text
            
        Returns:
            Plain text string
        """
        try:
            # Remove HTML tags
            text = re.sub('<[^<]+?>', '', html_text)
            # Remove extra whitespace
            text = ' '.join(text.split())
            # Remove special HTML entities
            text = text.replace('&nbsp;', ' ')
            text = text.replace('&amp;', '&')
            text = text.replace('&lt;', '<')
            text = text.replace('&gt;', '>')
            return text
            
        except Exception as e:
            return html_text
    
    def _get_owner_name(self, item: dict) -> str:
        """Extract owner name from item"""
        try:
            owner = item.get('Owner', {})
            if owner:
                return owner.get('_refObjectName', '')
            return ''
        except:
            return ''
    
    def _get_tags(self, item: dict) -> str:
        """Extract and format tags"""
        try:
            tags = item.get('Tags', {})
            if tags and tags.get('_tagsNameArray'):
                tag_list = tags['_tagsNameArray']
                return ', '.join([t.get('Name', '') for t in tag_list])
            return ''
        except:
            return ''
    
    def _get_release_name(self, item: dict) -> str:
        """Extract release name"""
        try:
            release = item.get('Release', {})
            if release:
                return release.get('Name', '')
            return ''
        except:
            return ''
    
    def _get_parent_name(self, item: dict) -> str:
        """Extract parent story name"""
        try:
            parent = item.get('Parent', {})
            if parent:
                return parent.get('FormattedID', '')
            return ''
        except:
            return ''
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse Rally date string to datetime object
        
        Args:
            date_str: Date string from Rally
            
        Returns:
            Datetime object or None
        """
        if not date_str:
            return None
        
        try:
            # Rally typically returns dates in ISO format
            return datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
        except:
            try:
                return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except:
                return None
    
    def insert_data_into_poc_table(
        self, 
        portfolio: str, 
        project_name: str, 
        conn
    ) -> bool:
        """
        Fetch data from Rally and insert into POC_Dashboard table
        
        Args:
            portfolio: Portfolio name
            project_name: Project name
            conn: Database connection
            
        Returns:
            Boolean indicating success
        """
        try:
            # Fetch data from Rally
            df = self.fetch_user_story_details(portfolio, project_name)
            
            if df.empty:
                print(f"No data to insert for project: {project_name}")
                return False
            
            # Prepare data for insertion
            cursor = conn.cursor()
            data_list = []
            
            for idx, item in df.iterrows():
                data_str = (
                    item['CreationDate'],
                    item['FormattedID'],
                    str(item['Name']).replace("\n", ""),
                    item['State'],
                    item['Owner'],
                    item['PlanEstimate'],
                    item['Iteration'],
                    item['IterationStartDate'],
                    item['IterationEndDate'],
                    item['SpilloverCount'],
                    item['SpilloverFlag'],
                    item['TaskCount'],
                    str(item['Details']).replace("\n", ""),
                    str(item['Notes']).replace("\n", ""),
                    item['CompletionDate'],
                    item['Priority'],
                    'Yes' if item['Blocked'] else 'No',
                    str(item['BlockedReason']).replace("\n", ""),
                    item['Tags'],
                    item['Release'],
                    item['Parent'],
                    portfolio,
                    project_name,
                    self.cio_info,
                    item['LastUpdateDate']
                )
                
                data_list.append(data_str)
            
            # Insert into database
            db_type = db_config.get_db_type()
            
            if db_type.lower() == constants.ORACLE:
                cursor.executemany(
                    db_config.INSERT_POC_DASHBOARD_QUERY,
                    data_list
                )
            else:
                cursor.executemany(
                    db_config.INSERT_POC_DASHBOARD_QUERY_POSTGRESQL,
                    data_list
                )
            
            conn.commit()
            print(f"Successfully inserted {len(data_list)} records for {project_name}")
            return True
            
        except Exception as e:
            print(f"Exception inserting data into POC table: {e}")
            if conn:
                conn.rollback()
            return False


def fetch_poc_dashboard_data_for_projects(datasets: List[dict], conn) -> None:
    """
    Main function to fetch POC Dashboard data for multiple projects
    
    Args:
        datasets: List of dataset dictionaries containing portfolio, project, API key info
        conn: Database connection
    """
    print("Starting POC Dashboard data fetch process...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=constants.MAXIMUM_WORKERS) as executor:
        futures = []
        
        for dataset in datasets:
            portfolio = dataset[constants.PORT_FOLIO]
            project_name = dataset[constants.PROJECT_NAME]
            api_key = dataset[constants.RALLY_API_KEY]
            cio_info = dataset[constants.CIO]
            
            # Create fetcher instance
            fetcher = POCDashboardDataFetcher(api_key, cio_info)
            
            # Submit task to thread pool
            future = executor.submit(
                fetcher.insert_data_into_poc_table,
                portfolio,
                project_name,
                conn
            )
            futures.append(future)
        
        # Wait for all tasks to complete
        for future in futures:
            try:
                result = future.result()
            except Exception as exc:
                print(f'Exception in POC data fetch: {exc}')
    
    elapsed_time = time.time() - start_time
    print(f"POC Dashboard data fetch completed in {elapsed_time:.2f} seconds")


# ========================================
# FILE 2: db_config.py (ADD THESE QUERIES)
# ========================================

# Add these constants to your existing db_config.py file

# Oracle INSERT query for POC Dashboard
INSERT_POC_DASHBOARD_QUERY = """
INSERT INTO POC_Dashboard (
    CreationDate,
    FormattedID,
    Name,
    State,
    Owner,
    PlanEstimate,
    Iteration,
    IterationStartDate,
    IterationEndDate,
    SpilloverCount,
    SpilloverFlag,
    TaskCount,
    Details,
    Notes,
    CompletionDate,
    Priority,
    Blocked,
    BlockedReason,
    Tags,
    Release,
    Parent,
    Portfolio,
    ProjectName,
    CIO,
    LastUpdateDate
) VALUES (
    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
    :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
    :21, :22, :23, :24, :25
)
"""

# PostgreSQL INSERT query for POC Dashboard
INSERT_POC_DASHBOARD_QUERY_POSTGRESQL = """
INSERT INTO POC_Dashboard (
    CreationDate,
    FormattedID,
    Name,
    State,
    Owner,
    PlanEstimate,
    Iteration,
    IterationStartDate,
    IterationEndDate,
    SpilloverCount,
    SpilloverFlag,
    TaskCount,
    Details,
    Notes,
    CompletionDate,
    Priority,
    Blocked,
    BlockedReason,
    Tags,
    Release,
    Parent,
    Portfolio,
    ProjectName,
    CIO,
    LastUpdateDate
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
"""

# CREATE TABLE query for POC Dashboard
CREATE_TABLE_POC_DASHBOARD = """
CREATE TABLE POC_Dashboard (
    ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CreationDate DATE,
    FormattedID VARCHAR2(50),
    Name VARCHAR2(500),
    State VARCHAR2(50),
    Owner VARCHAR2(200),
    PlanEstimate NUMBER(10,2),
    Iteration VARCHAR2(200),
    IterationStartDate DATE,
    IterationEndDate DATE,
    SpilloverCount NUMBER(5),
    SpilloverFlag VARCHAR2(10),
    TaskCount NUMBER(5),
    Details VARCHAR2(500),
    Notes VARCHAR2(1000),
    CompletionDate DATE,
    Priority VARCHAR2(50),
    Blocked VARCHAR2(10),
    BlockedReason VARCHAR2(500),
    Tags VARCHAR2(500),
    Release VARCHAR2(200),
    Parent VARCHAR2(50),
    Portfolio VARCHAR2(200),
    ProjectName VARCHAR2(200),
    CIO VARCHAR2(200),
    LastUpdateDate DATE,
    InsertedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# PostgreSQL CREATE TABLE
CREATE_TABLE_POC_DASHBOARD_POSTGRESQL = """
CREATE TABLE IF NOT EXISTS POC_Dashboard (
    ID SERIAL PRIMARY KEY,
    CreationDate TIMESTAMP,
    FormattedID VARCHAR(50),
    Name VARCHAR(500),
    State VARCHAR(50),
    Owner VARCHAR(200),
    PlanEstimate DECIMAL(10,2),
    Iteration VARCHAR(200),
    IterationStartDate TIMESTAMP,
    IterationEndDate TIMESTAMP,
    SpilloverCount INTEGER,
    SpilloverFlag VARCHAR(10),
    TaskCount INTEGER,
    Details VARCHAR(500),
    Notes VARCHAR(1000),
    CompletionDate TIMESTAMP,
    Priority VARCHAR(50),
    Blocked VARCHAR(10),
    BlockedReason VARCHAR(500),
    Tags VARCHAR(500),
    Release VARCHAR(200),
    Parent VARCHAR(50),
    Portfolio VARCHAR(200),
    ProjectName VARCHAR(200),
    CIO VARCHAR(200),
    LastUpdateDate TIMESTAMP,
    InsertedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Check if table exists query
CHECK_POC_TABLE_EXISTS = """
SELECT table_name 
FROM all_tables 
WHERE table_name = 'POC_DASHBOARD'
"""

# Check if table exists query (PostgreSQL)
CHECK_POC_TABLE_EXISTS_POSTGRESQL = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_name = 'poc_dashboard'
"""


# ========================================
# FILE 3: constants.py (ADD THESE CONSTANTS)
# ========================================

# Add these constants to your existing constants.py file

# POC Dashboard Column List for Rally API
POC_DASHBOARD_COLUMN_LIST = [
    'FormattedID',
    'Name',
    'CreationDate',
    'ScheduleState',
    'Owner',
    'PlanEstimate',
    'Iteration',
    'Description',
    'Notes',
    'AcceptedDate',
    'InProgressDate',
    'Tasks',
    'Priority',
    'Blocked',
    'BlockedReason',
    'Tags',
    'LastUpdateDate',
    'Release',
    'Parent'
]

# Database field names
SPILLOVER_COUNT = 'SpilloverCount'
SPILLOVER_FLAG = 'SpilloverFlag'
TASK_COUNT = 'TaskCount'
DETAILS = 'Details'
NOTES = 'Notes'
COMPLETION_DATE = 'CompletionDate'


# ========================================
# FILE 4: create_poc_table.py (NEW FILE)
# ========================================

"""
Script to create POC_Dashboard table in the database
Run this once to set up the table structure
"""

import db_config

def create_poc_dashboard_table(conn):
    """
    Create POC_Dashboard table if it doesn't exist
    
    Args:
        conn: Database connection
    """
    try:
        cursor = conn.cursor()
        db_type = db_config.get_db_type()
        
        # Check if table exists
        if db_type.lower() == 'oracle':
            cursor.execute(db_config.CHECK_POC_TABLE_EXISTS)
            table_exists = cursor.fetchone()
            
            if not table_exists:
                print("Creating POC_Dashboard table...")
                cursor.execute(db_config.CREATE_TABLE_POC_DASHBOARD)
                conn.commit()
                print("POC_Dashboard table created successfully!")
            else:
                print("POC_Dashboard table already exists.")
        else:
            cursor.execute(db_config.CHECK_POC_TABLE_EXISTS_POSTGRESQL)
            table_exists = cursor.fetchone()
            
            if not table_exists:
                print("Creating POC_Dashboard table...")
                cursor.execute(db_config.CREATE_TABLE_POC_DASHBOARD_POSTGRESQL)
                conn.commit()
                print("POC_Dashboard table created successfully!")
            else:
                print("POC_Dashboard table already exists.")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"Error creating POC_Dashboard table: {e}")
        return False


if __name__ == '__main__':
    conn = db_config.get_db_connection(db_config.get_db_type())
    create_poc_dashboard_table(conn)
    if conn:
        conn.close()


# ========================================
# FILE 5: scheduler.py (UPDATE MAIN FUNCTION)
# ========================================

# Add this to your main scheduler function in scheduler.py

def db_main_function():
    """
    Main execution function - Update this in your scheduler.py
    """
    try:
        start_time = time.time()
        
        # Get Connection
        db_type = db_config.get_db_type()
        conn = db_config.get_db_connection(db_type)
        
        # Check if tables exist, if not create them
        tables = create_tables_if_not_exists(conn)
        
        if tables:
            try:
                # Read configuration file
                json_path = "path/to/DigitalDashboard_config.json"
                print(f"Reading config from: {json_path}")
                
                with open(json_path, "r", encoding="utf-8") as config_file:
                    config = json.load(config_file)
                
                datasets = config.get('datasets', [])
                
                # EXISTING FETCHES
                # Update rally tables data
                start_time_update = time.time()
                update_rally_tables_data(conn)
                elapsed_time_update = time.time() - start_time_update
                print(f"Rally tables update completed in {elapsed_time_update:.2f} seconds")
                
                # Fetch artifacts for projects (existing code)
                with ThreadPoolExecutor(max_workers=constants.MAXIMUM_WORKERS) as executor:
                    future_data = {
                        executor.submit(
                            fetch_artifacts_for_project, 
                            dataset, 
                            conn
                        ): dataset for dataset in datasets
                    }
                    
                    for future in concurrent.futures.as_completed(future_data):
                        dataset = future_data[future]
                        try:
                            data = future.result()
                        except Exception as exc:
                            print(f'Exception in Rally Job: {exc}')
                
                # NEW: Fetch POC Dashboard data
                from DB_Fetch_POC_Dashboard import fetch_poc_dashboard_data_for_projects
                fetch_poc_dashboard_data_for_projects(datasets, conn)
                
                elapsed_time = time.time() - start_time
                print(f"Complete job completed in {elapsed_time:.2f} seconds")
                
            except Exception as e:
                print(f'Exception in Rally Job: {e}')
        
        if conn:
            conn.close()
            
    except Exception as e:
        print(f'Exception in db_main_function: {e}')


# ========================================
# FILE 6: Sample Config JSON
# ========================================

"""
Add this structure to your DigitalDashboard_config.json file:

{
  "datasets": [
    {
      "portfolio": "Technology",
      "project_name": "Digital Transformation",
      "milestone_name": "Sprint 1",
      "rally_api_key": "your_api_key_here",
      "cio": "John Doe"
    },
    {
      "portfolio": "Innovation",
      "project_name": "Cloud Migration",
      "milestone_name": "Phase 2",
      "rally_api_key": "your_api_key_here",
      "cio": "Jane Smith"
    }
  ]
}
"""


# ========================================
# FILE 7: Testing Script (test_poc_fetch.py)
# ========================================

"""
Test script to verify POC Dashboard data fetching
"""

import json
from DB_Fetch_POC_Dashboard import POCDashboardDataFetcher
import db_config

def test_poc_fetch():
    """Test POC Dashboard data fetching"""
    
    # Test configuration
    test_config = {
        'portfolio': 'Test Portfolio',
        'project_name': 'Test Project',
        'api_key': 'your_test_api_key',
        'cio_info': 'Test CIO'
    }
    
    # Create fetcher
    fetcher = POCDashboardDataFetcher(
        test_config['api_key'], 
        test_config['cio_info']
    )
    
    # Test data fetch
    print("Testing POC Dashboard data fetch...")
    df = fetcher.fetch_user_story_details(
        test_config['portfolio'],
        test_config['project_name']
    )
    
    if not df.empty:
        print(f"✓ Successfully fetched {len(df)} records")
        print("\nSample data:")
        print(df.head())
        
        print("\nColumn names:")
        print(df.columns.tolist())
        
        print("\nSpillover Statistics:")
        print(f"Stories with spillover: {df[df['SpilloverFlag'] == 'Yes'].shape[0]}")
        print(f"Average spillover count: {df['SpilloverCount'].mean():.2f}")
        print(f"Average task count: {df['TaskCount'].mean():.2f}")
    else:
        print("✗ No data fetched")
    
    return df

if __name__ == '__main__':
    test_poc_fetch()
