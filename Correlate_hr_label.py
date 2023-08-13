import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Load heart-related data from CSV
heart_data_file = "../CSV_Data/hr_input.csv"
heart_df = pd.read_csv(heart_data_file)

# Load emotion labels data from CSV
emotion_labels_file = "../CSV_Data/label_input.csv"
emotion_df = pd.read_csv(emotion_labels_file)

heart_df = heart_df.set_index('timestamp')
emotion_df = emotion_df.set_index('time')

# Merge the DataFrames based on the common timestamp
merged_df = pd.concat([heart_df, emotion_df], axis=1, join='inner')

# Calculate the correlation
correlation_results = merged_df.corr()

# Plot the correlation in a heatmap
sns.heatmap(correlation_results, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation between Heart Data and Emotion Labels")
plt.show()
