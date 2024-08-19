import os

import camelot
import pandas as pd
import logging
from unidecode import unidecode

from configs.rules.notas import rules_dict
logging.basicConfig(level=logging.INFO)

from pdf_etl.configs.tools.postgre import RDSPostgreSQLManager

class PDFTableExtractor:
    def __init__(self, file_name, configs):
        self.path = os.path.abspath(f"pdf_etl/files/pdf/{configs["name"].lower()}/{file_name}.pdf")
        self.csv_path = os.path.abspath(f"src/files/csv/")
        self.file_name = file_name
        self.configs = configs      
        
    def start():
        logging.info(f"Start pdf - {self.file_name}")
        
        header = self.get_table_detail()
        main = self.get_table_data()
        small = self.get_table_data()
        
        main = self.add_infos(header, main) 
        small = self.add_infos(header, small)
        
        main = self.sanitize_column_names(main)
        small = self.sanitize_column_names(small)
        
        self.save_csv(main, self.file_name)
        self.save_csv(small, f"{self.file_name}_small")

        logging.info(f"Sending to DB - {self.file_name}")
        self.send_to_db(main, f"Fatura_{self.configs['name']}".lower())
        self.send_to_db(small, f"Fatura_{self.configs['name']}_small".lower())
        
             
    def get_table_data(self, t_area, t_columns):
        tables = camelot.read_pdf(
            self.path,
            flavor=self.config["flavor"],
            table_areas=t_area,
            columns=t_columns,
            strip_text=self.configs["strip_text"],
            page=self.config["page"],
            password=self.config["password"]
        )
        
        table_content = [self.fix_header(page.df) if fix else page.df for page in tables]
        result = pd.concat(tabble_content_ , ignore_index=True) if len(table_content)> 1 else table_content[0]
        return result
        
    def save_csv():
        if not os.path.exists(self.csv_path):
            os.mkdir(self.csv_path, exist_ok=True)
        path = os.path.join[self.csv_path, f"(file_name).csv"]
        df.to_csv(path, sep=";", index=False)
        
    
    def add_infos(self, content):
        infos = header.iloc[0]
        df = pd.DataFrame([infos.values] * len(content), columns=header.columns)
        content = pd.concat([content.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
        content['Data de Inserção'] = pd.Timestamp("today").normalize()
        return content  
    
    @staticmethod
    def fix_header():
        df.column = df.iloc[0]
        df = df.drop(0)
        df = df.drop(df.column, axis=1)
        return df 

    def sanitize_column_names(self,df):
        df.column = df.columns.map(lambda x: unidecode(x))
        df.columns = df.columns.replace("", "_")
        df.columns = df.columns.str.replace(r'\W', '', regex=True)
        df.columns = df.columns.str.lower()
        return df
    
    def sent_to_ddb(df, table_name):
        try:
            connection = RDSPostgreSQLManager().alchemy
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logging.info(f"Dados salvos no DB{table_name}")
        except Exception as e:
            logging.error(e)
        
    
if __name__ == "__main__":
    pass    