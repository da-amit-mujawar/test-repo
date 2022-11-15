from typing import List
from typing_extensions import TypedDict


class SalesforceTable(TypedDict):
    object_name: str
    excluded_fields: List[str]


class SalesforceObject:
    def __init__(self):
        self._regular_table_list: List[SalesforceTable] = [
            {"object_name": "Opportunity", "excluded_fields": []},
            {"object_name": "Lead", "excluded_fields": []},
            {"object_name": "Account", "excluded_fields": []},
            {"object_name": "Contact", "excluded_fields": []},
            {"object_name": "Campaign", "excluded_fields": []},
            {"object_name": "CampaignMember", "excluded_fields": []},
        ]
        self._non_pk_chunking_table_list: List[SalesforceTable] = [
        ]

    @property
    def regular_table_list(self):
        return self._regular_table_list

    @property
    def non_pk_chunking_table_list(self):
        return self._non_pk_chunking_table_list
