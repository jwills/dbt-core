from dataclasses import dataclass, field
from dbt.dataclass_schema import dbtClassMixin

from typing import List, Literal, Union


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
    type: Literal["path", "query", "header", "body"]
    required: bool = False


@dataclass
class EndpointDefinition(dbtClassMixin):
    name: str
    path: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    args: List[ArgumentDefinition] = field(default_factory=list)


@dataclass
class ExternalServiceDefinition(dbtClassMixin):
    name: str
    base_url: str
    auth: AuthMethodSpec
    endpoints: List[EndpointDefinition] = field(default_factory=list)


@dataclass
class ExternalServiceFile(dbtClassMixin):
    external_services: List[ExternalServiceDefinition]
    version: int = 2
