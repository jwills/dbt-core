from dataclasses import dataclass, field
from dbt.dataclass_schema import dbtClassMixin

from typing import List, Union


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


AuthMethodSpec = Union[BasicAuthMethod, BearerAuthMethod]


@dataclass
class ArgumentDefinition(dbtClassMixin):
    name: str
    type: str  # one of query, body, header, or path
    required: bool = False


@dataclass
class EndpointDefinition(dbtClassMixin):
    name: str
    path: str
    method: str  # one of GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS
    args: List[ArgumentDefinition] = field(default_factory=list)


@dataclass
class ExternalServiceDefinition(dbtClassMixin):
    name: str
    base_url: str
    auth: AuthMethodSpec
    endpoints: List[EndpointDefinition] = field(default_factory=list)


@dataclass
class ExternalServiceFile(dbtClassMixin):
    services: List[ExternalServiceDefinition]
    version: int = 2
