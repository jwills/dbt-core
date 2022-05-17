from typing import Dict


class Auth:
    pass


class Endpoint:
    pass


class ExternalService:
    def __init__(self, name: str, base_url: str, auth: Auth, endpoints: Dict[str, Endpoint]):
        self.name = name
        self.base_url = base_url
        self.auth = auth
        self.endpoints = endpoints
        self._is_authenticated = False

    def authenticate(self):
        self._is_authenticated = True

    def endpoint(self, name: str):
        if not self._is_authenticated:
            self.authenticate()
        return self.endpoints[name]
