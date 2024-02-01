import pandas as pd
from utils import (
    replace_slashes_by_underscores,
    remove_table_prefix,
    to_snake_case,
    extract_first_number,
    remove_number_between_underscroes,
)
from creds import *
from sqlalchemy import create_engine
import mysql.connector

field_actions_csv_path = "input/field_actions.csv"
airbnb_scraped_csv = "input/2024_01_31_airbnb_scraper_data_set.csv"

def pull_field_actions_df():
    field_actions = pd.read_csv(field_actions_csv_path)
    field_actions["Field"] = field_actions["Field"].apply(
        replace_slashes_by_underscores
    )
    field_actions["Action"] = field_actions["Action"].apply(remove_table_prefix)
    field_actions = field_actions[field_actions["Action"] != "PRIMARY KEY"]
    field_actions = field_actions[field_actions["Action"] != "Do Not Import"]
    print("Fielad Actions DF:\n", field_actions.head(3))
    return field_actions


def get_tables_columns_dict(field_actions: pd.DataFrame):
    tables_columns = {}
    for i, row in field_actions.iterrows():
        action = row["Action"]
        field = row["Field"]
        field = replace_slashes_by_underscores(to_snake_case(field))
        processing_action = row["Processing Action"]
        if action not in tables_columns:
            tables_columns[action] = {}
            tables_columns[action]["columns"] = []
            tables_columns[action]["columns"].append("guid")
            tables_columns[action]["one-to-many"] = processing_action == "One:Many"

        tables_columns[action]["columns"].append(field)
    print("Tables: \n", tables_columns.keys())
    return tables_columns


def get_scraped_data_df():
    airbnb_scrape = pd.read_csv(airbnb_scraped_csv)
    airbnb_scrape.columns = map(to_snake_case, airbnb_scrape.columns)
    airbnb_scrape.columns = map(replace_slashes_by_underscores, airbnb_scrape.columns)
    airbnb_scrape.rename(columns={"id_str": "guid"}, inplace=True)
    print("Number of Columns: ", len(airbnb_scrape.columns))
    print("Number of Unique Columns: ", len(airbnb_scrape.columns.unique()))
    return airbnb_scrape


def normalize_data_model(tables_columns: dict, airbnb_parent_df: pd.DataFrame):
    child_tables = {}
    for table_name in tables_columns:
        child_table_df = airbnb_parent_df[tables_columns[table_name]["columns"]]
        child_tables[table_name] = (
            child_table_df,
            tables_columns[table_name]["one-to-many"],
        )
    return child_tables


def pivot_one_to_many_tables(child_tables: dict):
    for table_name in child_tables:
        is_one_to_many = child_tables[table_name][1]
        if is_one_to_many:
            df = child_tables[table_name][0]
            melted_df = pd.melt(
                df, id_vars=["guid"], var_name="column", value_name="value"
            )
            melted_df[f"{table_name}_number"] = melted_df["column"].apply(
                extract_first_number
            )
            melted_df["column"] = melted_df["column"].apply(
                remove_number_between_underscroes
            )
            melted_df_pivoted = melted_df.pivot(
                index=["guid", f"{table_name}_number"], columns="column", values="value"
            ).reset_index()
            if table_name == "listing_rooms":
                listing_rooms_lvl_1_columns = [
                    "listing_rooms_number",
                    "listing_rooms_room_number",
                    "listing_rooms_id",
                ]
                listing_rooms_lvl_1 = melted_df_pivoted[
                    ["guid"] + listing_rooms_lvl_1_columns
                ]
                listing_rooms_lvl_1 = listing_rooms_lvl_1.dropna()
                listing_rooms_lvl_2 = melted_df_pivoted.drop(
                    columns=listing_rooms_lvl_1_columns
                )
                listing_rooms_lvl_2 = (
                    listing_rooms_lvl_2.set_index("guid")
                    .dropna(how="all")
                    .reset_index()
                )
                listing_rooms_lvl_2["listing_rooms_id"] = listing_rooms_lvl_2[
                    "listing_rooms_beds_0_id"
                ].apply(lambda x: int(x.split("/")[0]))

                listing_rooms_lvl_2_melted_df = pd.melt(
                    listing_rooms_lvl_2,
                    id_vars=["guid", "listing_rooms_id"],
                    var_name="column",
                    value_name="value",
                    ignore_index=True,
                )
                listing_rooms_lvl_2_melted_df[
                    f"room_number"
                ] = listing_rooms_lvl_2_melted_df["column"].apply(extract_first_number)
                listing_rooms_lvl_2_melted_df["column"] = listing_rooms_lvl_2_melted_df[
                    "column"
                ].apply(remove_number_between_underscroes)

                listing_rooms_lvl_2_melted_df_pivoted = (
                    listing_rooms_lvl_2_melted_df.pivot(
                        index=["guid", "listing_rooms_id", "room_number"],
                        columns="column",
                        values="value",
                    )
                    .reset_index()
                    .dropna()
                )
                merged_lvl_1_and_2 = pd.merge(
                    listing_rooms_lvl_1,
                    listing_rooms_lvl_2_melted_df_pivoted,
                    on=["guid", "listing_rooms_id"],
                    how="left",
                )
                print(merged_lvl_1_and_2.head(3))
                listing_rooms_final_columns = [
                    "guid",
                    "listing_rooms_number",
                    "listing_rooms_id",
                    "listing_rooms_room_number",
                    "listing_rooms_beds_id",
                    "listing_rooms_beds_type",
                    "listing_rooms_beds_quantity",
                ]
                listing_rooms_final = merged_lvl_1_and_2[listing_rooms_final_columns]
                listing_rooms_final = listing_rooms_final.loc[
                    listing_rooms_final.index.repeat(
                        listing_rooms_final["listing_rooms_beds_quantity"]
                    )
                ].reset_index(drop=True)
                listing_rooms_final = listing_rooms_final.drop(
                    columns=["listing_rooms_beds_quantity"]
                )
                child_tables[table_name] = (listing_rooms_final, True)
                continue
            child_tables[table_name] = (melted_df_pivoted, True)
    return child_tables


def write_tables_in_mysql(tables, black_list_tables: list = []):
    connection = mysql.connector.connect(**db_config)
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{DB}")
    for table_name in tables:
        airbnb_table_name = f"airbnb_{table_name}"
        if table_name in black_list_tables:
            continue
        if table_name == "listing_rooms":
            df = tables[table_name][0]
            print(f"Writing table {airbnb_table_name}...")
            df.to_sql(airbnb_table_name, con=engine, if_exists="replace", index=False)
        else:
            df = tables[table_name][0]
            print(f"Writing table {airbnb_table_name}...")
            df.to_sql(airbnb_table_name, con=engine, if_exists="replace", index=False)
    connection.close()


if __name__ == "__main__":
    actions_df = pull_field_actions_df()
    tables_columns = get_tables_columns_dict(actions_df)
    airbnb_parent_df = get_scraped_data_df()
    child_tables = normalize_data_model(tables_columns, airbnb_parent_df)
    treated_child_tables = pivot_one_to_many_tables(child_tables)
    write_tables_in_mysql(treated_child_tables)
