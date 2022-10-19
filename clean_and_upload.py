from distutils.command.upload import upload
from sales_retail_loader.clean_functions import *
from sql_functions import *
from sales_disti_loader.clean_disti import *

def my_menu():
    print()
    print("Explicación: Toma el archivo, le da formato y lo sube a nuestra base de datos.¿")
    print()
    print("Elige la acción que quieres realizar:")
    print()
    print("(1) Limpiar y subir ABC")
    print("(2) Limpiar y subir E-shop")
    print("(3) Limpiar y subir Falabella")
    print("(4) Limpiar y subir Hites")
    print("(5) Limpiar y subir La Polar")
    print("(6) Limpiar y subir Paris")
    print("(7) Limpiar y subir PcFactory")
    print("(8) Limpiar y subir Ripley")
    print("(9) Limpiar y subir Walmart")
    print("(10) Limpiar y subir Intcomex")
    print("(11) Limpiar y subir Ingram")
    print("(12) Limpiar y subir Nexsys")
    print("(0) Salir")
    print()
    choice = int(input("Ingresa tu opción: "))
    print()

    while (choice != 0):
        if choice == 1:
            clean_abc()
            upload_retail()
        elif choice == 2:
            clean_eshop()
            upload_retail()
        elif choice == 3:
            clean_falabella()
            upload_retail()
        elif choice == 4:
            clean_hites()
            upload_retail()
        elif choice == 5:
            clean_lapolar()
            upload_retail()
        elif choice == 6:
            clean_paris()
            upload_retail()
        elif choice == 7:
            clean_pcfactory()
            upload_retail()
        elif choice == 8:
            clean_ripley()
            upload_retail()
        elif choice == 9:
            clean_walmart()
            upload_retail()
        elif choice == 10:
            clean_intcomex()
            upload_disti()
        elif choice == 11:
            clean_ingram()
            upload_disti()
        elif choice == 12:
            clean_nexsys()
            upload_disti()
        else:
            print("Favor ingresar un número")
        choice = int(input("Si quieres salir presiona 0 o selecciona otra opción: "))

my_menu()