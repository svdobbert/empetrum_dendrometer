import pandas as pd
import numpy as np

# Parameters
min_shrink_pct = 10   # Minimum shrinkage as % of individual amplitude
max_duration = 30     # Maximum days over which shrinkage can occur
min_duration = 2      # Minimum duration to count as an event
rebound_tolerance = 3 # Max consecutive days of increase tolerated within event

# Load and prepare data
df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])
df = df.dropna(subset=["D_mean"])

# Add time features
df["year"] = df["date"].dt.year
df["doy"] = df["date"].dt.dayofyear

# Calculate amplitude (range) for each ID
df["amp"] = df.groupby("id")["normalized"].transform(
    lambda s: (s.max() - s.min())
)

# Calculate daily change
df["change"] = df.groupby("id")["D_mean"].diff()

print(f"Data loaded: {len(df)} rows for {df['id'].nunique()} unique IDs")
print(df.head())


def detect_shrinking_events(g):
    """
    Detect shrinking events: periods where stem diameter drops by at least 
    min_shrink_pct% of amplitude, with tolerance for brief rebounds.
    
    Algorithm:
    1. Start at each point in the time series
    2. Look forward up to max_duration days
    3. Check if cumulative drop meets threshold
    4. Allow brief rebounds (consecutive increases) up to rebound_tolerance
    5. Record the event when threshold is met with minimum duration
    """
    dates = g["date"].to_numpy()
    values = g["D_mean"].to_numpy()
    amp = g["amp"].iloc[0]
    
    if pd.isna(amp) or amp == 0:
        return []
    
    threshold = amp * (min_shrink_pct / 100.0)
    
    events = []
    n = len(values)
    used = np.zeros(n, dtype=bool)  
    
    i = 0
    while i < n:
        if used[i]:
            i += 1
            continue
            
        start_val = values[i]
        start_idx = i
        
        j = i + 1
        consecutive_increases = 0
        min_val = start_val
        min_idx = i
        
        # Look forward for potential shrinking event
        while j < n and (j - i) <= max_duration:
            current_val = values[j]
            
            # Track minimum value reached
            if current_val < min_val:
                min_val = current_val
                min_idx = j
                consecutive_increases = 0
            elif current_val > values[j-1]:
                # Diameter increased from previous day
                consecutive_increases += 1
                if consecutive_increases > rebound_tolerance:
                    # Too many consecutive increases, end search
                    break
            else:
                consecutive_increases = 0
        
            total_shrink = start_val - min_val
            
            if total_shrink >= threshold:
                # Found a valid event from i to min_idx
                duration = min_idx - start_idx + 1
                
                if duration >= min_duration:
                    # Record this event
                    events.append({
                        'start_idx': start_idx,
                        'end_idx': min_idx,
                        'start_date': dates[start_idx],
                        'end_date': dates[min_idx],
                        'start_val': start_val,
                        'end_val': min_val,
                        'shrinkage': total_shrink,
                        'shrinkage_pct': (total_shrink / amp) * 100,
                        'duration': duration
                    })
                    
                    # Mark these days as used
                    used[start_idx:min_idx+1] = True
                    
                    # Continue search after this event
                    i = min_idx + 1
                    break
            
            j += 1
        else:
            # Didn't find valid event starting at i
            i += 1
    
    return events


# Detect events for all IDs
all_events = []

for id_, g in df.groupby("id"):
    g = g.sort_values("date").reset_index(drop=True)
    
    events = detect_shrinking_events(g)
    
    for event in events:
        start_ts = pd.Timestamp(event['start_date'])
        end_ts = pd.Timestamp(event['end_date'])
        
        all_events.append({
            "id": id_,
            "year": start_ts.year,
            "start": start_ts,
            "stop": end_ts,
            "length_days": event['duration'],
            "start_doy": start_ts.dayofyear,
            "stop_doy": end_ts.dayofyear,
            "start_D_mean": event['start_val'],
            "stop_D_mean": event['end_val'],
            "total_shrink": event['shrinkage'],
            "shrink_pct": event['shrinkage_pct']
        })

print(f"\nDetected {len(all_events)} shrinking events")

# Create dataframe
if len(all_events) > 0:
    events_df = pd.DataFrame(all_events)
    events_df = events_df.sort_values(["id", "start"])
    
    print("\nEvent statistics:")
    print(f"Mean duration: {events_df['length_days'].mean():.1f} days")
    print(f"Mean shrinkage: {events_df['shrink_pct'].mean():.1f}% of amplitude")
    print(f"\nSample of detected events:")
    print(events_df.head(10))
    
    # Save results
    events_df.to_csv("input/shrinking-events.csv", index=False)
    print("\nResults saved to input/shrinking-events.csv")
else:
    print("\nNo events detected. Consider adjusting parameters:")
    print(f"  - min_shrink_pct: {min_shrink_pct}%")
    print(f"  - max_duration: {max_duration} days")
    print(f"  - min_duration: {min_duration} days")
    print(f"  - rebound_tolerance: {rebound_tolerance} days")
    