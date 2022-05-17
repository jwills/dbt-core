from dataclasses import dataclass, field
from typing import Dict, List

import base64
import requests


@dataclass
class Auth:
    headers: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def basic(cls, username: str, password: str) -> "Auth":
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        return cls(headers={"Authorization": "Basic " + token})

    @classmethod
    def bearer(cls, token: str) -> "Auth":
        return cls(headers={"Authorization": "Bearer " + token})


@dataclass
class Argument:
    name: str
    type: str
    required: bool = False


@dataclass
class Endpoint:
    name: str
    path: str
    method: str
    args: List[Argument] = field(default_factory=list)


@dataclass
class ExternalService:
    name: str
    base_url: str
    auth: Auth
    endpoints: Dict[str, Endpoint] = field(default_factory=dict)

    def request(
        self, session: requests.Session, endpoint_name: str, **kwargs
    ) -> requests.Response:
        endpoint = self.endpoints[endpoint_name]
        headers = self.auth.headers.copy()
        path_args, query_args, body_args = {}, {}, {}
        for arg in endpoint.args:
            if arg.name not in kwargs:
                if arg.required:
                    raise ValueError(f"Missing required argument {arg.name}")
            else:
                value = kwargs.pop(arg.name)
                if arg.type == "path":
                    path_args[arg.name] = value
                elif arg.type == "query":
                    query_args[arg.name] = value
                elif arg.type == "body":
                    body_args[arg.name] = value
                elif arg.type == "header":
                    headers[arg.name] = value
                else:
                    raise ValueError(f"Unknown argument type {arg.type} for {arg.name}")
        url = self.base_url + endpoint.path.format(**path_args)
        return session.request(
            endpoint.method, url, headers=headers, params=query_args, json=body_args
        )
