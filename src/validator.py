import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Validate:
    def __init__(self, config_object):
        self.config_object = config_object
    
    def df_customers_validator(self, df):
        logger.info("Starting customer dataframe validation...")

        columns_yaml = self.config_object.config_data["sources"][0]["schema"]
        for column_name, col_type in columns_yaml.items():
            if column_name not in df.columns:
                logger.error(f"Missing column in customers dataframe: {column_name}")
                df["reject_reason"] = f"column {column_name} is missing"
                return (pd.DataFrame(), df)

            if col_type in ("float", "int"):
                logger.info(f"Converting column {column_name} to numeric.")
                df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
            elif col_type == "datetime":
                logger.info(f"Converting column {column_name} to datetime.")
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")

        rules_yaml = self.config_object.config_data["sources"][0]["rules"]
        df_filtered = df
        df_rejects = pd.DataFrame()

        for rule in rules_yaml:
            rule_text = rule["rule"]
            logger.info(f"Applying customer rule: {rule_text}")

            rule_violators = df_filtered.query(f"not ({rule_text})").copy()

            if not rule_violators.empty:
                logger.warning(
                    f"Customer validation rule failed: {rule_text} "
                    f"→ {len(rule_violators)} rows rejected."
                )
                rule_violators["reject_reason"] = f"not passing {rule_text} rule"
                df_rejects = pd.concat([df_rejects, rule_violators])

            df_filtered = df_filtered.query(rule_text)

        logger.info(
            f"Customer dataframe validation complete → "
            f"{len(df_filtered)} rows passed, {len(df_rejects)} rows rejected."
        )

        return (df_filtered, df_rejects)
            

    def df_sales_validator(self, df):
        logger.info("Starting sales dataframe validation...")

        columns_yaml = self.config_object.config_data["sources"][1]["schema"]
        for column_name, col_type in columns_yaml.items():

            if column_name not in df.columns:
                logger.error(f"Missing column in sales dataframe: {column_name}")
                df["reject_reason"] = f"column {column_name} is missing"
                return (0, df)

            if col_type in ("float", "int"):
                logger.info(f"Converting column {column_name} to numeric.")
                df[column_name] = pd.to_numeric(df[column_name], errors="coerce")

            elif col_type == "datetime":
                logger.info(f"Converting column {column_name} to datetime.")
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")

        rules_yaml = self.config_object.config_data["sources"][1]["rules"]
        df_filtered = df
        df_rejects = pd.DataFrame()

        for rule in rules_yaml:
            rule_text = rule["rule"]
            logger.info(f"Applying sales rule: {rule_text}")

            rule_violators = df_filtered.query(f"not ({rule_text})").copy()

            if not rule_violators.empty:
                logger.warning(
                    f"Sales validation rule failed: {rule_text} "
                    f"→ {len(rule_violators)} rows rejected."
                )
                rule_violators["reject_reason"] = f"not passing {rule_text} rule"
                df_rejects = pd.concat([df_rejects, rule_violators])

            df_filtered = df_filtered.query(rule_text)

        logger.info(
            f"Sales dataframe validation complete → "
            f"{len(df_filtered)} rows passed, {len(df_rejects)} rows rejected."
        )

        return (df_filtered, df_rejects)