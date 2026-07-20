import psycopg2

from pyspark.sql.functions import (
    col,
    lit,
    array,
    when
)


# ==========================================
# Get Validation Rules from PostgreSQL
# ==========================================
def get_validation_rules(dataset):

    connection = psycopg2.connect(
        host="rules-postgres",
        port=5432,
        database="rules_db",
        user="rules_user",
        password="rules_password"
    )

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            column_name,
            column_type,
            operator,
            value
        FROM validation_rules
        WHERE dataset = %s
        """,
        (dataset,)
    )

    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    rules = []

    for row in rows:
        rules.append({
            "column_name": row[0],
            "column_type": row[1],
            "operator": row[2],
            "value": row[3]
        })

    return rules


# ==========================================
# Convert Validation Value
# ==========================================
def convert_value(value, column_type):

    if value is None:
        return None

    if column_type == "integer":
        return int(value)

    if column_type in [
        "float",
        "double",
        "decimal"
    ]:
        return float(value)

    return value


# ==========================================
# Create Spark Validation Condition
# ==========================================
def create_condition(rule):

    column_name = rule["column_name"]
    column_type = rule["column_type"]
    operator = rule["operator"]

    value = convert_value(
        rule["value"],
        column_type
    )

    column = col(column_name)

    if operator == ">":
        return column > value

    elif operator == "<":
        return column < value

    elif operator == ">=":
        return column >= value

    elif operator == "<=":
        return column <= value

    elif operator in [
        "==",
        "="
    ]:
        return column == value

    elif operator == "!=":
        return column != value

    else:
        raise ValueError(
            f"Unsupported operator: {operator}"
        )


# ==========================================
# Apply Validation Rules
# ==========================================
def apply_validation_rules(df, rules):

    if not rules:
        print(
            "WARNING: No validation rules found."
        )

        return df, df.limit(0)

    conditions = []

    failure_messages = []

    for rule in rules:

        column_name = rule["column_name"]
        operator = rule["operator"]
        value = rule["value"]

        # Make sure the column exists
        if column_name not in df.columns:

            print(
                f"Skipping rule. "
                f"Column '{column_name}' "
                f"does not exist."
            )

            continue

        condition = create_condition(
            rule
        )

        conditions.append(
            condition
        )

        failure_message = (
            f"{column_name} must satisfy "
            f"{operator} {value}"
        )

        failure_messages.append(
            when(
                condition == True,
                lit(None)
            ).otherwise(
                lit(failure_message)
            )
        )

    if not conditions:

        print(
            "WARNING: No valid conditions "
            "could be created."
        )

        return df, df.limit(0)

    # ==========================================
    # Combine All Conditions
    # ==========================================
    final_condition = conditions[0]

    for condition in conditions[1:]:

        final_condition = (
            final_condition & condition
        )


    # ==========================================
    # Good Records
    # ==========================================
    good_df = df.filter(
        final_condition == True
    )


    # ==========================================
    # Add Failure Reasons
    # ==========================================
    validated_df = df.withColumn(
        "failure_reasons",
        array(
            *failure_messages
        )
    )


    # ==========================================
    # Bad Records
    # ==========================================
    bad_df = validated_df.filter(
        (final_condition == False)
        |
        final_condition.isNull()
    )


    return good_df, bad_df