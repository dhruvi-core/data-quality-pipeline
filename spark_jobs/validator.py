from rules import (
    equity_rules,
    bond_rules,
    commodity_rules
)


def validate_data(df, division):

    if division == "equity":
        return equity_rules(df)

    elif division == "bond":
        return bond_rules(df)

    elif division == "commodity":
        return commodity_rules(df)

    else:
        raise Exception(f"Unknown division : {division}")