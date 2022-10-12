from sales_retail_loader.clean_functions import *
from sales_retail_loader.sql_functions import *

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
    print("(0) Salir")
    print()
    choice = int(input("Ingresa tu opción: "))
    print()

    while (choice != 0):
        if choice == 1:
            clean_abc()
            upload()
        elif choice == 2:
            clean_eshop()
            upload()
        elif choice == 3:
            clean_falabella()
            upload()
        elif choice == 4:
            clean_hites()
            upload()
        elif choice == 5:
            clean_lapolar()
            upload()
        elif choice == 6:
            clean_paris()
            upload()
        elif choice == 7:
            clean_pcfactory()
            upload()
        elif choice == 8:
            clean_ripley()
            upload()
        elif choice == 9:
            clean_walmart()
            upload()
        else:
            print("Favor ingresar un número")
        choice = int(input("Si quieres salir presiona 0 o selecciona otra opción: "))

my_menu()