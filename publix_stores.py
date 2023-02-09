from utils.file_readers import FileHandler, Path, np, pd


publix_csv = FileHandler(Path.cwd() / "spreadsheets" / "20230209_publix_locations.csv")

publix_excel = FileHandler(
    Path.cwd() / "spreadsheets" / "StoreDirectoryReport 8.8.22.xlsx",
)

publix_excel.read(using_pandas=True, skiprows=3)
# publix_excel.convert_nulls()

data = publix_excel.data
data = data.dropna(axis=1, how="all")
data = data.rename(columns={"unnamed:_5": "state", "unnamed:_7": "zip"})

for index, row in data.iterrows():
    store_num = row["store_no."]
    if str(store_num).isnumeric() or store_num is pd.notnull(store_num):
        data.at[index, "abstract_store_num"] = row["store_no."]
    elif store_num is pd.isnull(store_num):
        data.at[index, "abstract_store_num"] = data.at[index - 1, "abstract_store_num"]

    if not str(store_num).isnumeric():
        data.at[index, "abstract_store_category"] = row["store_no."]

for col in [
    "abstract_store_num",
    "abstract_store_category",
    "zip",
    "state",
    "regional_director",
    "district_manager",
    "store_phone_number",
    "county",
]:
    data[col] = data[col].fillna(method="ffill")

# Fill first Store Number
data.at[0, "abstract_store_num"] = data["abstract_store_num"].ffill().iloc[1]
data.at[0, "zip"] = data["zip"].ffill().iloc[2]
data.at[0, "state"] = data["state"].ffill().iloc[2]
data.at[1, "zip"] = data["zip"].ffill().iloc[2]
data.at[1, "state"] = data["state"].ffill().iloc[2]

reset_idx = data.set_index(["abstract_store_num", "abstract_store_category"])
reset_idx["abstract_location"] = " ".join(
    [str(x) for x in reset_idx["location"].values]
)

for each in reset_idx.index:
    string_list = []
    for addr in reset_idx.loc[each, "location"]:
        string_list.append(str(addr))

    special_services = []
    for service in reset_idx.loc[each, "special_services"]:
        if service is not np.nan:
            special_services.append(str(service))
    reset_idx.loc[each, "abstract_location"] = " ".join(string_list).replace("nan", "")
    reset_idx.loc[each, "abstract_services"] = ", ".join(
        [x.strip() for x in special_services]
    )

reset_idx = reset_idx.reset_index()
reset_idx["abstract_store_num"] = reset_idx["abstract_store_num"].str.zfill(4)
# Change column order to abstract at front
reset_idx = reset_idx[
    [
        "abstract_store_category",
        "abstract_store_num",
        "abstract_location",
        "abstract_services",
        "zip",
        "state",
        "regional_director",
        "district_manager",
        "store_phone_number",
        "county",
        "location",
        "special_services",
    ]
]
reset_idx.to_csv(
    Path.home() / "Downloads" / "20230209_skinny_original_publix.csv", index=False
)

uniques = reset_idx.drop_duplicates(
    subset=["abstract_store_num", "abstract_store_category"]
)
uniques = uniques.drop(columns=["special_services", "location"])
uniques.to_csv(Path.home() / "Downloads" / "20230209_deduped_publix.csv", index=False)
