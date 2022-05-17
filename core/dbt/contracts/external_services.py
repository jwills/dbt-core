from dataclasses import dataclass, field
from dbt.dataclass_schema import dbtClassMixin

from typing import List, Literal, Optional, Union


@dataclass
class AuthMethod(dbtClassMixin):
    pass


@dataclass
class BasicAuthMethod(AuthMethod):
    username: str
    password: str


@dataclass
class BearerAuthMethod(AuthMethod):
    token: str


@dataclass
class ClientCredentialsAuthMethod(AuthMethod):
    client_id: str
    client_secret: str


AuthMethodSpec = Union[BasicAuthMethod, BearerAuthMethod, ClientCredentialsAuthMethod]


@dataclass
class Param(dbtClassMixin):
    name: str
    type: Literal["path", "query", "header", "body"]
    required: bool = False


@dataclass
class Endpoint(dbtClassMixin):
    name: str
    path: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    args: List[Param] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class ExternalServiceDefinition(dbtClassMixin):
    name: str
    base_url: str
    auth: AuthMethodSpec
    endpoints: List[Endpoint] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class ExternalServiceFile(dbtClassMixin):
    external_services: List[ExternalServiceDefinition]
    version: int = 2
