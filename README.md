# Slowly Changing Dimension Type 2 (scd2) Custom Materialization dbt Package

## What does this dbt package do?

This dbt package provides a new materialization that builds an advanced version of slowly changing dimension type 2 (SCD2):

- A new record is added if there is a change in the **check_cols** column list just like it's done in the original ```check_dbt_snapshot``` strategy.

- It uses **updated_at** column like in the original timestamp dbt snapshot strategy to define the time limits when a record is valid (valid_from - valid_to columns).

- You can load data in a batch-like one-time historical initial load. The batch may contain several versions of the same entity or even duplicates (with the same **unique_key** and **updated_at**). There is deduplication embedded in the logic.

- If there is not a complete duplicate record (with the same **unique_key** and **updated_at** but different **check_cols**), the logic can use **loaded_at** (the timestamp for when the data was loaded in the staging area) to update **check_cols** with the most recent known values.

- The dimension is loaded incrementally. This means that if a target table does not exist, it's created from the first data batch/row. Otherwise, new records are inserted and the existing ones are updated if needed.

- The load process supports **_out of order_** transactions. This means that if there is already an entity version in the dimension for a specific time period as well as ones that come later, you shall receive a new version of the same entity for the part of the existing time period in your staging area (for changes occuring "in the past" on the already loaded data), the existing record in the dimension must be split in 2.

- Along with the Kimball Type II setting in **check_cols** , you can configure Kimball Type I setting in **punch_thru_cols** column list. These attributes are updated in **all** dimension record versions .

- **update_cols** column lists are updated only in the **last** dimension record version.

- **LoadDate** and **UpdateDate** can be populated from **loaddate** value provided (variable).

- The first entity record **valid_from** in the dimension can be the first **updated_at** value (default) or any timestamp you provide in **scd_valid_from_min_date** . Setting **scd_valid_from_min_date** to **1900-01-01** allows to use the first entity record in a fact table transaction with transaction dates before the entity first **updated_at** value e.g. before the entity was born. You can use any date older then the oldest possible **updated_at** value.

- The last entities record  **valid_to** value in the dimension is **NULL** by default, but you can override it with **scd_valid_to_max_date** . Setting **scd_valid_to_max_date** to something like **3000-01-01** will simplify joining fact records to the dimension avoiding **NULLs** in joins. You can use any date in a future after the latest possible **updated_at** value.

- The materialization does not handle soft deletes and does not provide for auto schema evolution (see also below).

- **scd2_plus** custom materialization does **not** support **model contracts**.

- There is also **scd2_plus_validation** test to check consistency in **valid_from** and **valid_to** . It means no gaps in or intersection of versions periods in an entity. If non-default names are set in **scd_valid_from_col_name** , **scd_valid_to_col_name**, they should be specified in the test.

Only the columns configured in **unique_key, updated_at**, **check_cols** , **punch_thru_cols**  and **update_cols** will be added in the target table. If there are other columns in the SELECT statement provided, they will be ignored.

Each **scd2_plus** materialized dimension always has these service columns:

- Dimension surrogate key (varchar(50)) is a combination of **unique_key** and **updated_at**. It's **scd_id** by default, but can be configured in **scd_id_col_name**.
- Dimension record version **start** timestamp is **valid_from** by default, but can be customized in **scd_valid_from_col_name**.
- Dimension record version **end** timestamp is **valid_to** by default, but can be customized in **scd_valid_to_col_name**.
- Dimension record version **ordering number** is **record_version** (integer) by default and custom column name can be configured in **scd_record_version_col_name**.
- Data loaded in a record at **Loaddate** timestamp, customizable in **scd_loaddate_col_name**.
- Data updated in a record at **UpdateDate** timestamp, customizable in **scd_updatedate_col_name**.
- **scd_hash** column (varchar(50)) is used to track changes in **check_cols**.

## How do I use the dbt package?

### Step 1: Prerequisites

To use this dbt package, you must have the following

- Postgres, Snowflake, Spark, BigQuery, DuckDB, or Trino destination.
- Staging data with a unique key and updated_at columns, and, obviously, columns to track history.


### Step 2: Install the package

Include the ```versioned_scd``` package in your packages.yml file.

```
packages:
  - git: "https://github.com/sudo-donya/versioned_scd"
```

and run

```
dbt deps
```

### Step 3: Configure model

**Minimum configuration**

```
    config:
      materialized: scd2_plus
      unique_key: id
      check_cols: [label, amount]
      updated_at: source_system_updated_date
      loaded_at: staging_load_date
    data_tests:
      - versioned_scd.scd2_plus_validation:
          unique_key: id
          scd_valid_from_col_name: valid_from
          scd_valid_to_col_name: valid_to

```

**Full customization**

```
    config:
      materialized: scd2_plus
      unique_key: id
      check_cols: [label, amount]
      updated_at: source_system_updated_date
      punch_thru_cols: [component_name, date_of_birth]
      update_cols: [component_description]
      loaded_at: staging_load_date
      scd_id_col_name: component_id
      scd_valid_from_col_name: valid_from_dt
      scd_valid_to_col_name: valid_to_dt
      scd_record_version_col_name: version
      scd_loaddate_col_name: loaded_at
      scd_updatedate_col_name: updated_at
      scd_valid_from_min_date: '1900-01-01'
      scd_valid_to_max_date: '3000-01-01'

```
_Do not forget to add_ **_loaddate_** _variable in dbt_project.yml._
```
vars:

loaddate: "1900-01-01"
```
### Step 4: Add test

In a schema.yml file:

```
models:
  - name: dim_scd2_plus_full_config
    tests:
      - versioned_scd.scd2_plus_validation:
          unique_key: 'id'
          scd_valid_from_col_name: 'valid_from_dt'
          scd_valid_to_col_name:  'valid_to_dt'

```
The out-of-box dbt uniqueness test is recommended for the dimension surrogate key column(**scd_id**).


### Step 5: Run dbt

```
dbt run
```
### Step 5: Run test

```
dbt test
```
