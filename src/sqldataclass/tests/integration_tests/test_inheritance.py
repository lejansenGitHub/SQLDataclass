"""Tests for single-table inheritance — v0.1.1 feature."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class InhVehicle(SQLDataclass, table=True):
    __tablename__ = "inh_vehicle"
    id: int | None = Field(default=None, primary_key=True)
    type: str = ""
    name: str = ""
    doors: int | None = None
    payload: float | None = None


class InhCar(InhVehicle, inherit=True, discriminator_column="type", discriminator_value="car"):
    pass


class InhTruck(InhVehicle, inherit=True, discriminator_column="type", discriminator_value="truck"):
    pass


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


class TestSingleTableInheritance:
    def test_insert_sets_discriminator(self, bound_engine: Any) -> None:
        InhCar(name="Civic", doors=4).insert()
        car = InhCar.load_one(where=InhCar.c.name == "Civic")
        assert car is not None
        assert car.type == "car"

    def test_subtype_load_all_filters(self, bound_engine: Any) -> None:
        InhCar(name="Civic").insert()
        InhCar(name="Model 3").insert()
        InhTruck(name="F-150").insert()

        cars = InhCar.load_all()
        assert len(cars) == 2
        assert all(c.type == "car" for c in cars)

        trucks = InhTruck.load_all()
        assert len(trucks) == 1
        assert trucks[0].type == "truck"

    def test_parent_load_all_returns_everything(self, bound_engine: Any) -> None:
        InhCar(name="Civic").insert()
        InhTruck(name="F-150").insert()
        all_vehicles = InhVehicle.load_all()
        assert len(all_vehicles) == 2

    def test_subtype_update_scoped(self, bound_engine: Any) -> None:
        InhCar(name="Civic", doors=4).insert()
        InhTruck(name="F-150", payload=1000.0).insert()

        InhCar.update({"doors": 2})
        car = InhCar.load_one(where=InhCar.c.name == "Civic")
        assert car is not None
        assert car.doors == 2

        # Truck unchanged
        truck = InhTruck.load_one(where=InhTruck.c.name == "F-150")
        assert truck is not None
        assert truck.payload == 1000.0

    def test_subtype_delete_scoped(self, bound_engine: Any) -> None:
        InhCar(name="Civic").insert()
        InhTruck(name="F-150").insert()

        InhTruck.delete()
        assert len(InhTruck.load_all()) == 0
        assert len(InhCar.load_all()) == 1
        assert len(InhVehicle.load_all()) == 1

    def test_subtype_load_one_with_where(self, bound_engine: Any) -> None:
        InhCar(name="Civic", doors=4).insert()
        InhCar(name="Model 3", doors=4).insert()
        InhTruck(name="Civic", payload=500.0).insert()  # same name, different type

        car = InhCar.load_one(where=InhCar.c.name == "Civic")
        assert car is not None
        assert car.type == "car"
        assert car.doors == 4

    def test_subtype_shares_parent_table(self, bound_engine: Any) -> None:
        assert InhCar.__table__ is InhVehicle.__table__
        assert InhTruck.__table__ is InhVehicle.__table__

    def test_subtype_with_pagination(self, bound_engine: Any) -> None:
        for i in range(5):
            InhCar(name=f"Car_{i}").insert()
        for i in range(3):
            InhTruck(name=f"Truck_{i}").insert()

        cars = InhCar.load_all(limit=2)
        assert len(cars) == 2

        trucks = InhTruck.load_all(limit=10)
        assert len(trucks) == 3
