import os
from PyPDF2 import PdfReader, PdfWriter
import pandas as pd
from datetime import datetime
from pathlib import Path

"""
function to merge all the pdf factsheets together
"""


def get_all_pdfs(folder_path):
    pdf_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.lower().endswith('.pdf'):
                pdf_files.append(file_path)
    return pdf_files


if __name__ == '__main__':
    folder_path = 'H:\Investor\Factsheets'
    pdf_files = get_all_pdfs(folder_path)
    df = pd.DataFrame(columns=['file_path'])
    df['file_path'] = pdf_files
    df['date'] = None
    for index, row in df.iterrows():
        name = row['file_path']
        month_str = name.split(" ")[1]
        year_str = name.split(" ")[2][:4]
        month_num = datetime.strptime(month_str, "%B").month
        first_day_of_month = datetime(int(year_str), month_num, 1)
        df.loc[index, 'date'] = first_day_of_month
    df = df.sort_values(by=['date'], ascending=False)
    pdf_merger = PdfWriter()
    for index, row in df.iterrows():
        pdf_path = Path(row['file_path'])
        with open(pdf_path, 'rb') as pdf_file:
            pdf_reader = PdfReader(pdf_file)
            for page_num in range(len(pdf_reader.pages)):
                pdf_merger.add_page(pdf_reader.pages[page_num])
    output_path = 'H:\Investor\Factsheets.pdf'
    with open(output_path, 'wb') as output_file:
        pdf_merger.write(output_file)


