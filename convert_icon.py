from PIL import Image

# Abrir tu PNG original
img = Image.open("icon.png")

# Guardar como .ico con varios tamaños
img.save("icon.ico", sizes=[(16,16), (32,32), (64,64)])
print("Icono convertido a icon.ico")
