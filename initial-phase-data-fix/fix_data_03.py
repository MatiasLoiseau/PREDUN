import pandas as pd

# Load the CSV file
input_file = 'data-private/CURSADA_HISTORICA_02.csv'
df = pd.read_csv(input_file)

df['NOTA'] = df['NOTA'].replace({'A': 7, 'R': 1}).astype(float)

# Save the modified data to a new CSV file
output_file = 'data-private/CURSADA_HISTORICA_final.csv'
df.to_csv(output_file, index=False)
print('Data saved to {}'.format(output_file))