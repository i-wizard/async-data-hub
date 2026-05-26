from pydantic import BaseModel


class HealthStatusResponse(BaseModel):
    """
    Describes the health of core infrastructure so startup and readiness checks
    prove that the same dependencies used by real requests are reachable.
    """

    status: str
    database: str
    cache: str
