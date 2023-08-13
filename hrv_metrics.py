import pandas as pd
import hrv

'''
neurokit verwenden => evtl hrv ersetzen
'''

def compute_hrv_metrics(ibi_data):
    # Compute HRV metrics
    hrv_results = hrv.frequency_domain(ibi_data)
    time_domain_metrics = hrv.time_domain(ibi_data)
    nonlinear_metrics = hrv.nonlinear(ibi_data)

    # Combine all metrics into a single dictionary
    all_metrics = {
        "Mean RR": time_domain_metrics["mean_nni"],
        "SDNN": time_domain_metrics["sdnn"],
        "RMSSD": time_domain_metrics["rmssd"],
        "NN50": time_domain_metrics["nn50"],
        "pNN50": time_domain_metrics["pnn50"],
        "LF Power": hrv_results["lf"],
        "HF Power": hrv_results["hf"],
        "LF/HF Ratio": hrv_results["lf/hf"],
        "SD1": nonlinear_metrics["sd1"],
        "SD2": nonlinear_metrics["sd2"],
        "Sample Entropy": nonlinear_metrics["sampen"],
        "Approximate Entropy": nonlinear_metrics["apen"],
    }

    return all_metrics

# Read data from CSV
csv_file = "../hr_input.csv"
df = pd.read_csv(csv_file)
hrv_metrics = compute_hrv_metrics(df['hrIbi'])

# Create a new DataFrame with original data and computed HRV metrics
df_hrv = df.copy()
for metric, value in hrv_metrics.items():
    df_hrv[metric] = value

# Write the new DataFrame with HRV metrics to a new CSV file
output_csv_file = "../CSV_Data/hrv_metrics.csv"
df_hrv.to_csv(output_csv_file, index=False)

print("HRV metrics calculated and saved to the new CSV file successfully.")