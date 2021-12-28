from dataclasses import dataclass


# Table Definitions

@dataclass
class ColumnDefinition:
    type: str
    length: str = None
    nullable: bool = None
    default: str = None


@dataclass
class Column:
    name: str
    definition: ColumnDefinition
