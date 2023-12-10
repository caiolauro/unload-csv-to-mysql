import re

replace_slashes_by_underscores = lambda value: value.replace("/", "_")
remove_table_prefix = lambda value: value.replace("Table: ", "")
field_actions_csv_path = "input/field_actions.csv"


def to_snake_case(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("__([A-Z])", r"_\1", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def extract_first_number(s):
    # Use a regular expression to find the first number in the string
    match = re.search(r"\d+", s)

    # If a match is found, return the first number as an integer
    if match:
        return int(match.group())

    # If no match is found, return None
    return None


def remove_number_between_underscroes(input_string):
    output_string = re.sub(r"(?<=_)\d+|\d+(?=_)", "", input_string, count=1).replace(
        "__", "_"
    )
    return output_string
