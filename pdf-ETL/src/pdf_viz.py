import os
import camelot
import pandas
import matplotlib
import matplotlib.pyplot as plt

file_name = 'corretora_jornada_de_dados (1)'
path = os.path.abspath(f"pdf_etl/files/pdf/jornada/{file_name}.pdf")

tables = camelot.read_pdf(
    path,
    pages='1-end', #pega da primeira página até o final
    flavor='stream', #tecnologia que vai usar para interpretar o pdf
    table_areas=['65, 558, 500, 298'],
    columns=["65, 107, 156, 212, 280, 336, 383, 450"],
    strip_text=".\n",
    password=""
    
)

print(tables[0].parsing_report)
    
#camelot.plot(tables[0], kind="contour")  #exibe como o software está enxergando o pdf

#plt.show() 

print(tables[0].df)
    
print("Pause")