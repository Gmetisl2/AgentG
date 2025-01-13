import base64

with open('user.session', 'rb') as file:
    encoded = base64.b64encode(file.read()).decode('utf-8')

part_size = 5000  # Adjust the size as needed
parts = [encoded[i:i + part_size] for i in range(0, len(encoded), part_size)]

for idx, part in enumerate(parts):
    with open(f'user.session.b64.part{idx+1}', 'w') as file:
        file.write(part)