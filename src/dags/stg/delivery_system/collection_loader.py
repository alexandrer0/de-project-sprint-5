from datetime import datetime, timedelta
from typing import Dict, List
import requests

class CollectionLoader:
    def __init__(self, api_url, headers) -> None:
        self.api_url = api_url
        self.headers = headers

    def get_documents(self, collection_name: str, sort_field: str, sort_direction: str, limit: int) -> List[Dict]:
        date_from = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        date_to = (datetime.utcnow()).strftime('%Y-%m-%d %H:%M:%S')
        offset = 0
        docs = []
        docs_offset = requests.get(
            url=self.api_url + f'/{collection_name}?from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
            headers=self.headers
        ).json()
        while docs_offset:
            docs += docs_offset
            offset += 50
            docs_offset = requests.get(
                url=self.api_url + f'/{collection_name}?from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                headers=self.headers
            ).json()
        return docs