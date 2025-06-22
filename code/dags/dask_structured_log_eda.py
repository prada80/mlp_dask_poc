from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
from io import BytesIO
import configuration
from dask.distributed import Client

def safe_int(val):
    try:
        return int(val.compute())
    except AttributeError:
        return int(val)

def analyse_request_id_feature(df):
    analysis_results = {}

    value_counts = df["request_id"].value_counts(dropna=False).head(50)
    analysis_results["request_id_top_50_value_counts"] = value_counts.to_dict()

    num_nulls = df["request_id"].isna().sum()
    analysis_results["request_id_null_count"] = safe_int(num_nulls)

    num_dash = (df["request_id"] == "-").sum()
    analysis_results["request_id_dash_count"] = safe_int(num_dash)

    has_duplicates = bool(df["request_id"].compute().duplicated().any())
    analysis_results["request_id_has_duplicates"] = has_duplicates

    print("\nAnalysis for 'request_id' column:")
    print(f"Top 50 value counts: {analysis_results['request_id_top_50_value_counts']}")
    print(f"Null count: {analysis_results['request_id_null_count']}")
    print(f"Dash count: {analysis_results['request_id_dash_count']}")
    print(f"Has duplicates: {analysis_results['request_id_has_duplicates']}")

    buffer = BytesIO()
    pd.DataFrame([analysis_results]).to_csv(buffer, index=False)
    boto3.client("s3").put_object(
        Bucket=configuration.DEST_BUCKET,
        Key=f"{configuration.EDA_OUTPUT}/eda_analyse_request_id_feature.csv",
        Body=buffer.getvalue()
    )

def analyse_feature_datatype_missing_value(df):
    print(" Columns:", df.columns)

    summary_dict = {
        "columns": ", ".join(df.columns),
        "row_count": safe_int(df.shape[0])
    }

    dtypes = df.dtypes.astype(str).to_dict()
    missing = df.isnull().sum().compute().to_dict()

    for col in df.columns:
        summary_dict[f"dtype_{col}"] = dtypes.get(col)
        summary_dict[f"missing_{col}"] = missing.get(col)

    buffer = BytesIO()
    pd.DataFrame([summary_dict]).to_csv(buffer, index=False)
    boto3.client("s3").put_object(
        Bucket=configuration.DEST_BUCKET,
        Key=f"{configuration.EDA_OUTPUT}/eda_summary.csv",
        Body=buffer.getvalue()
    )

def analyse_feature_histogram(df):
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    for col in numeric_cols:
        plt.figure()
        df[col].compute().hist(bins=30)
        plt.title(f"Histogram of {col}")
        plt.xlabel(col)
        plt.ylabel("Frequency")

        img_buffer = BytesIO()
        plt.savefig(img_buffer, format='png')
        img_buffer.seek(0)

        file_key = f"{configuration.EDA_OUTPUT}/histogram_{col}.png"
        boto3.client("s3").put_object(
            Bucket=configuration.DEST_BUCKET,
            Key=file_key,
            Body=img_buffer,
            ContentType='image/png'
        )
        plt.close()
        print(f"Histogram saved to s3://{configuration.DEST_BUCKET}/{file_key}")

def impute_request_id(df):
    print("\nPerforming imputation for '-' values in 'request_id' with 'rca-system'...")
    df_copy = df.copy()
    updated = df_copy['request_id'].replace('-', "rca-system").compute()
    df_copy['request_id'] = dd.from_pandas(updated, npartitions=df_copy.npartitions)
    print("Imputation complete.")
    return df_copy

def perform_dask_eda_and_save_to_s3(**kwargs):
    try:
        print("Starting perform_dask_eda_and_save_to_s3:")
        client = Client("tcp://dask-scheduler.dask.svc.cluster.local:8786")

        s3_path = f"s3://{configuration.DEST_BUCKET}/{configuration.SILVER_FILE_KEY}"
        df = dd.read_csv(s3_path)
        print("CSV file read from S3.")

        # Ensure divisions are known before any partition-based operations
        if not df.known_divisions:
            df = df.set_index(df.columns[0], sorted=False, drop=False)

        analyse_feature_datatype_missing_value(df)
        analyse_feature_histogram(df)

        if 'request_id' in df.columns:
            analyse_request_id_feature(df)
            df = impute_request_id(df)

            imputed_output_path = f"s3://{configuration.DEST_BUCKET}/{configuration.SILVER_FILE_KEY}"
            print(f"Writing imputed DataFrame back to {imputed_output_path}...")
            df.to_csv(imputed_output_path, single_file=True, index=False)
            print("Imputed structured log saved successfully.")
        else:
            print("Column 'request_id' not found in the dataset. Skipping request_id analysis.")

        print("EDA summary and plots saved to S3.")
        client.close()
    except Exception as e:
        print(f"Error during EDA processing: {e}")

# DAG Start Time (rounded down to nearest 30 mins minus 5 mins)
now_utc = datetime.now(timezone.utc)
start_date_utc = now_utc.replace(minute=(now_utc.minute // 30) * 30, second=0, microsecond=0) - timedelta(minutes=5)
with DAG(
    dag_id='Step_2_rca_structured_log_eda',
    start_date=start_date_utc,
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=['s3', 'validation', 'etl'],
) as dag:
    eda_task = PythonOperator(
        task_id="perform_dask_eda_and_save_to_s3",
        python_callable=perform_dask_eda_and_save_to_s3
    )
