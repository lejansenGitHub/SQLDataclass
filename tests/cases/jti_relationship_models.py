"""Module-level JTI models with relationships for edge case testing.

Defined at module level so get_type_hints can resolve forward references.
"""

from sqldataclass import Field, Relationship, SQLDataclass

# ---------------------------------------------------------------------------
# One-to-many: Manager (JTI child) → Report
# ---------------------------------------------------------------------------


class PersonO2M(SQLDataclass, table=True):
    __tablename__ = "jti_persons_o2m"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class ReportO2M(SQLDataclass, table=True):
    __tablename__ = "jti_reports_o2m"
    id: int | None = Field(default=None, primary_key=True)
    title: str = ""
    manager_id: int = Field(foreign_key="jti_managers_o2m.id")


class ManagerO2M(PersonO2M, table=True):
    __tablename__ = "jti_managers_o2m"
    department: str = ""
    reports: list[ReportO2M] = Relationship()


# ---------------------------------------------------------------------------
# Many-to-one: Employee (JTI child) → Location
# ---------------------------------------------------------------------------


class LocationM2O(SQLDataclass, table=True):
    __tablename__ = "jti_locations_m2o"
    id: int | None = Field(default=None, primary_key=True)
    city: str = ""


class PersonM2O(SQLDataclass, table=True):
    __tablename__ = "jti_persons_m2o"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class EmployeeM2O(PersonM2O, table=True):
    __tablename__ = "jti_employees_m2o"
    role: str = ""
    location_id: int | None = Field(default=None, foreign_key="jti_locations_m2o.id")
    location: LocationM2O | None = Relationship()
