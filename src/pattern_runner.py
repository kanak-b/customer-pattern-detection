from src.patterns import pat1_upgrade, pat2_child, pat3_dei_needed
from datetime import datetime
import pytz


def get_current_ist_time():
    """
    Return the current timestamp in Asia/Kolkata timezone as a string.
    """
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')


def run_pattern_detections(transactions_df, cust_imp_df):
    """
    Run all 3 pattern detections and return a combined Spark DataFrame.
    
    Args:
        transactions_df (DataFrame): Input transaction data
        cust_imp_df (DataFrame): Customer importance data

    Returns:
        DataFrame: Combined detection results
    """
    ist_time = get_current_ist_time()

    pat1_df = pat1_upgrade.detect(transactions_df, cust_imp_df, ist_time)
    pat2_df = pat2_child.detect(transactions_df, ist_time)
    pat3_df = pat3_dei_needed.detect(transactions_df, ist_time)

    return pat1_df.unionByName(pat2_df).unionByName(pat3_df)
