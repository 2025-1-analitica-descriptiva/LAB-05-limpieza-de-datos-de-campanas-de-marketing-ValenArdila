"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os 
import glob
import csv 
import zipfile
import pandas as pd


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """
    #Lectura de archivos sin descomprimirlos
    dataframes = []
    
    for index in range(10):
        df = pd.read_csv(f"./files/input/bank-marketing-campaing-{str(index)}.csv.zip")
        dataframes.append(df)
    
    #Concatenacion de todos los archivos en un solo dataframe
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    #Limpieza client
    df_client = combined_df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    df_client["job"] = df_client["job"].str.replace(".", "").str.replace("-", "_")
    df_client["education"] = df_client["education"].str.replace(".", "_").replace("unknown", pd.NA)
    df_client["credit_default"] = df_client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    df_client["mortgage"] = df_client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    
    
    #Limpieza de campaign
    df_campaign = combined_df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", 
                               "campaign_outcome"]]
    df_campaign["previous_outcome"] = df_campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    df_campaign["last_contact_date"] = pd.to_datetime("2022-" + combined_df["month"].astype(str)+ "-" +combined_df["day"].astype(str))
    
    #Limpieza economics
    df_economics = combined_df[["client_id", "cons_price_idx", "euribor_three_months"]]
    
    #Guardado de dataframes
    output_directory = "./files/output"
    if os.path.exists(output_directory):
        files = glob.glob(f"{output_directory}/*")
        for file in files:
            os.remove(file)
        os.rmdir(output_directory)
    
    os.mkdir(output_directory)
    df_client.to_csv(os.path.join(output_directory, "client.csv"), index=False)
    df_campaign.to_csv(os.path.join(output_directory, "campaign.csv"), index=False)
    df_economics.to_csv(os.path.join(output_directory, "economics.csv"), index=False)
    

if __name__ == "__main__":
    clean_campaign_data()
