#!/usr/bin/env python3
"""
TPC-H Performance Visualization Script

Creates performance plots for TPC-H queries across different scale factors.
Each query gets its own subplot showing PostgreSQL, Arrow Flight SQL, and Substrait execution times.
"""

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
from pathlib import Path

# Configuration
SCRIPT_DIR = Path(__file__).parent
SUBSTRAIT_TEST_DIR = SCRIPT_DIR / "substrait_test"
QUERIES = ["Q01", "Q03", "Q05", "Q06", "Q09", "Q18"]
SCALE_FACTORS = [0.01, 0.1, 1, 5, 10, 15, 20]

def load_tpch_data():
    """Load all TPC-H timing CSV files and combine them into a single DataFrame."""
    all_data = []
    
    for sf in SCALE_FACTORS:
        csv_file = SUBSTRAIT_TEST_DIR / f"timing_tpch_sf{sf}.csv"
        if csv_file.exists():
            df = pd.read_csv(csv_file)
            all_data.append(df)
        else:
            print(f"Warning: {csv_file} not found, skipping SF={sf}")
    
    if not all_data:
        raise FileNotFoundError("No TPC-H timing CSV files found!")
    
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df

def plot_tpch_performance(data, output_file="tpch_performance.png"):
    """
    Create a 2x3 grid of plots, one for each TPC-H query.
    Each plot shows 3 curves: PostgreSQL, Arrow Flight SQL, and Substrait execution times.
    """
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('TPC-H Query Performance Across Scale Factors', fontsize=16, fontweight='bold')
    
    # Flatten axes for easier iteration
    axes = axes.flatten()
    
    # Color scheme for the three execution methods
    colors = {
        'pg_time_s': '#1f77b4',      # Blue
        'arrow_time_s': '#ff7f0e',   # Orange
        'substrait_time_s': '#2ca02c' # Green
    }
    
    labels = {
        'pg_time_s': 'PostgreSQL',
        'arrow_time_s': 'Arrow Flight SQL',
        'substrait_time_s': 'Substrait'
    }
    
    markers = {
        'pg_time_s': 'o',
        'arrow_time_s': 's',
        'substrait_time_s': '^'
    }
    
    for idx, query in enumerate(QUERIES):
        ax = axes[idx]
        
        # Filter data for this query
        query_data = data[data['query'] == query].sort_values('sf')
        
        if query_data.empty:
            ax.text(0.5, 0.5, f'No data for {query}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(query, fontweight='bold')
            continue
        
        # Plot each timing metric
        for metric in ['pg_time_s', 'arrow_time_s', 'substrait_time_s']:
            # Filter out NaN values for this metric
            valid_data = query_data[query_data[metric].notna()]
            
            if not valid_data.empty:
                ax.plot(valid_data['sf'], valid_data[metric], 
                       marker=markers[metric], 
                       color=colors[metric],
                       label=labels[metric],
                       linewidth=2,
                       markersize=8,
                       alpha=0.8)
        
        # Customize subplot
        ax.set_xlabel('Scale Factor (SF)', fontsize=11, fontweight='bold')
        ax.set_ylabel('Execution Time (seconds)', fontsize=11, fontweight='bold')
        ax.set_title(query, fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(loc='best', framealpha=0.9)
        
        # Use log scale for both axes if the range is large
        if not query_data.empty:
            max_time = query_data[['pg_time_s', 'arrow_time_s', 'substrait_time_s']].max().max()
            min_time = query_data[['pg_time_s', 'arrow_time_s', 'substrait_time_s']].min().min()
            
            # Use log scale if range spans more than 2 orders of magnitude
            if max_time / min_time > 100:
                ax.set_yscale('log')
                ax.set_ylabel('Execution Time (seconds, log scale)', fontsize=11, fontweight='bold')
        
        # Set x-axis to log scale for better visualization
        ax.set_xscale('log')
        ax.set_xlabel('Scale Factor (SF, log scale)', fontsize=11, fontweight='bold')
        
        # Format x-axis ticks
        ax.set_xticks(SCALE_FACTORS)
        ax.set_xticklabels([str(sf) for sf in SCALE_FACTORS])
    
    plt.tight_layout()
    
    # Save the figure
    output_path = SCRIPT_DIR / output_file
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")
    
    # Display the plot
    plt.show()

def main():
    """Main function to load data and create plots."""
    print("Loading TPC-H timing data...")
    data = load_tpch_data()
    
    print(f"Loaded {len(data)} data points")
    print(f"Scale factors: {sorted(data['sf'].unique())}")
    print(f"Queries: {sorted(data['query'].unique())}")
    
    print("\nCreating performance plots...")
    plot_tpch_performance(data)
    
    print("\nDone!")

if __name__ == "__main__":
    main()

# Made with Bob
