from aes_test import prpcrypt

aes = prpcrypt('keyskeyskeyskeys', '1234567812345678')
ciphertext = aes.encrypt("121.15.171.90")
print(ciphertext)
plain_text = aes.decrypt(ciphertext)
print(plain_text)