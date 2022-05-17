from pathlib import Path
from typing import Any, Dict, List
from dbt.clients.yaml_helper import yaml, Loader, Dumper, load_yaml_text  # noqa: F401
from dbt.dataclass_schema import ValidationError

from .renderer import ExternalServiceRenderer

from dbt.clients.system import (
    load_file_contents,
    path_exists,
    resolve_path_from_base,
)
from dbt.contracts.external_services import (
    AuthMethod,
    BasicAuthMethod,
    BearerAuthMethod,
    EndpointDefinition,
    ExternalServiceDefinition,
    ExternalServiceFile,
)
from dbt.exceptions import DbtExternalServiceError, RuntimeException
from dbt.external_services import Argument, Auth, Endpoint, ExternalService


class ExternalServiceConfig(Dict[str, ExternalService]):
    @classmethod
    def external_services_from_dict(
        cls, data: Dict[str, Dict[str, Any]]
    ) -> "ExternalServiceConfig":
        try:
            ExternalServiceFile.validate(data)
            external_service_file = ExternalServiceFile.from_dict(data)
            external_services = parse_from_external_services_definition(
                external_service_file.services
            )
        except ValidationError as exc:
            yaml_sel_cfg = yaml.dump(exc.instance)
            raise DbtExternalServiceError(
                f"Could not parse external service file data: \n{yaml_sel_cfg}\n",
                result_type="invalid_external_service_file",
            ) from exc
        except RuntimeException as exc:
            raise DbtExternalServiceError(
                f"Could not read external service file data: {exc}",
                result_type="invalid_external_service_file",
            ) from exc
        return cls(external_services)

    @classmethod
    def render_from_dict(
        cls,
        data: Dict[str, Any],
        renderer: ExternalServiceRenderer,
    ) -> "ExternalServiceConfig":
        try:
            rendered = renderer.render_data(data)
        except (ValidationError, RuntimeException) as exc:
            raise DbtExternalServiceError(
                f"Could not render external service data: {exc}",
                result_type="invalid_external_service",
            ) from exc
        return cls.external_services_from_dict(rendered)

    @classmethod
    def from_path(
        cls,
        path: Path,
        renderer: ExternalServiceRenderer,
    ) -> "ExternalServiceConfig":
        try:
            data = load_yaml_text(load_file_contents(str(path)))
        except (ValidationError, RuntimeException) as exc:
            raise DbtExternalServiceError(
                f"Could not read external service file: {exc}",
                result_type="invalid_external_service",
                path=path,
            ) from exc

        try:
            return cls.render_from_dict(data, renderer)
        except DbtExternalServiceError as exc:
            exc.path = path
            raise


def external_service_data_from_root(project_root: str) -> Dict[str, Any]:
    external_service_filepath = resolve_path_from_base("external_services.yml", project_root)

    if path_exists(external_service_filepath):
        external_service_dict = load_yaml_text(load_file_contents(external_service_filepath))
    else:
        external_service_dict = None
    return external_service_dict


def external_service_config_from_data(
    external_service_data: Dict[str, Any]
) -> ExternalServiceConfig:
    if not external_service_data:
        external_service_data = {"services": []}

    try:
        services = ExternalServiceConfig.external_services_from_dict(external_service_data)
    except ValidationError as e:
        raise DbtExternalServiceError(
            "Could not parse external service file data: {}".format(e),
            result_type="invalid_external_service",
        ) from e
    return services


def parse_auth(auth_definition: AuthMethod) -> Auth:
    if isinstance(auth_definition, BasicAuthMethod):
        return Auth.basic(auth_definition.username, auth_definition.password)
    elif isinstance(auth_definition, BearerAuthMethod):
        return Auth.bearer(auth_definition.token)
    else:
        raise RuntimeException(f"Unknown auth type: {auth_definition}")


def parse_endpoint(endpoint_definition: EndpointDefinition) -> Endpoint:
    args = []
    for arg_definition in endpoint_definition.args:
        args.append(
            Argument(
                name=arg_definition.name,
                type=arg_definition.type,
                required=arg_definition.required,
            )
        )

    return Endpoint(
        name=endpoint_definition.name,
        path=endpoint_definition.path,
        method=endpoint_definition.method,
        args=args,
    )


def parse_external_service(
    external_service_definition: ExternalServiceDefinition,
) -> ExternalService:
    auth = parse_auth(external_service_definition.auth)
    endpoints = {}
    for endpoint_definition in external_service_definition.endpoints:
        endpoints[endpoint_definition.name] = parse_endpoint(endpoint_definition)
    return ExternalService(
        name=external_service_definition.name,
        base_url=external_service_definition.base_url,
        auth=auth,
        endpoints=endpoints,
    )


def parse_from_external_services_definition(
    external_services_definition: List[ExternalServiceDefinition],
) -> Dict[str, ExternalService]:
    external_services = {}
    for external_service in external_services_definition:
        external_services[external_service.name] = parse_external_service(external_service)
    return external_services
