using CSV
using DataFrames
using Dates
using Statistics

# Parameters for OUTER events (depth 0)
const MIN_SHRINK_PCT = 10.0
const MAX_DURATION = 60
const MIN_DURATION = 2
const REBOUND_TOLERANCE = 15
const RECOVERY_PCT = 95.0

# Parameters for NESTED events (depth 1+)
const NESTED_MIN_SHRINK_PCT = 15.0   
const NESTED_RECOVERY_PCT = 80.0   
const MAX_NESTING_DEPTH = 10

# Load and prepare data 
println("Loading data...")
df = CSV.read("input/daily_data_with_trends.csv", DataFrame)
df.date = Date.(df.date)
sort!(df, [:id, :date])
dropmissing!(df, :D_mean)

df.year = year.(df.date)
df.doy = dayofyear.(df.date)

df = combine(groupby(df, :id)) do g
    first_val = first(g.D_mean[.!ismissing.(g.D_mean)])
    g.D_base = g.D_mean .- first_val
    g
end

df = combine(groupby(df, :id)) do g
    amp = maximum(g.D_base) - minimum(g.D_base)
    g.amp .= amp
    g
end

sort!(df, [:id, :date])
df.change = Vector{Union{Missing,Float64}}(missing, nrow(df))
for g in groupby(df, :id)
    rows = parentindices(g)[1]
    for i in 2:length(rows)
        df.change[rows[i]] = df.D_mean[rows[i]] - df.D_mean[rows[i-1]]
    end
end

println("Data loaded: $(nrow(df)) rows for $(length(unique(df.id))) unique IDs")

function detect_shrinking_events(dates::AbstractVector{Date}, values::AbstractVector{Float64}, 
                                 amp::Float64; min_shrink_pct::Float64=MIN_SHRINK_PCT, 
                                 recovery_pct::Float64=RECOVERY_PCT)
    """
    Detect shrinking events with configurable thresholds.
    """
    if isnan(amp) || amp == 0
        return DataFrame()
    end

    threshold = amp * (min_shrink_pct / 100.0)
    println("  Detecting events: min shrink = $(round(threshold, digits=2)) units ($(min_shrink_pct)%), recovery = $(recovery_pct)%")
    n = length(values)
    used = falses(n)

    events = DataFrame(
        start_idx=Int[],
        end_idx=Int[],
        recovery_idx=Int[],
        start_date=Date[],
        end_date=Date[],
        recovery_date=Date[],
        start_val=Float64[],
        end_val=Float64[],
        recovery_val=Float64[],
        shrinkage=Float64[],
        shrink_threshold=Float64[],
        shrinkage_pct=Float64[],
        duration=Int[],
        recovery_duration=Int[]
    )

    i = 1
    while i <= n
        if used[i]
            i += 1
            continue
        end

        start_val = values[i]
        start_idx = i
        j = i + 1
        consecutive_increases = 0
        min_val = start_val
        min_idx = i

        while j <= n && (j - i) <= MAX_DURATION
            current_val = values[j]

            if current_val < min_val
                min_val = current_val
                min_idx = j
                consecutive_increases = 0
            elseif j > 1 && current_val > values[j-1]
                consecutive_increases += 1
                if consecutive_increases > REBOUND_TOLERANCE
                    break
                end
            else
                consecutive_increases = 0
            end

            total_shrink = start_val - min_val

            if total_shrink >= threshold
                duration = min_idx - start_idx + 1

                if duration >= MIN_DURATION
                    # Find local maximum before the minimum
                    actual_start_idx = start_idx
                    actual_start_val = start_val
                    for k in start_idx:min_idx-1
                        if values[k] > actual_start_val
                            actual_start_val = values[k]
                            actual_start_idx = k
                        end
                    end

                    # Recalculate shrinkage with actual start
                    total_shrink = actual_start_val - min_val
                    recovery_threshold = min_val + (total_shrink * recovery_pct / 100.0)
                    recovery_idx = min_idx
                    recovery_val = min_val

                    k = min_idx + 1
                    while k <= n
                        if values[k] >= recovery_threshold
                            recovery_idx = k
                            recovery_val = values[k]
                            break
                        end
                        k += 1
                    end

                    if recovery_idx == min_idx && min_idx < n
                        recovery_idx = n
                        recovery_val = values[n]
                    end

                    recovery_duration = recovery_idx - actual_start_idx + 1

                    push!(events, (
                        start_idx=actual_start_idx,
                        end_idx=min_idx,
                        recovery_idx=recovery_idx,
                        start_date=dates[actual_start_idx],
                        end_date=dates[min_idx],
                        recovery_date=dates[recovery_idx],
                        start_val=actual_start_val,
                        end_val=min_val,
                        recovery_val=recovery_val,
                        shrinkage=total_shrink,
                        shrink_threshold=threshold,
                        shrinkage_pct=(total_shrink / amp) * 100,
                        duration=min_idx - actual_start_idx + 1,
                        recovery_duration=recovery_duration
                    ))

                    used[start_idx:recovery_idx] .= true
                    i = recovery_idx + 1
                    break
                end
            end

            j += 1
        end

        if j > n || (j - i) > MAX_DURATION || consecutive_increases > REBOUND_TOLERANCE
            i += 1
        end
    end

    return events
end

# Recursive function with depth-specific parameters
function detect_nested_events(id_val, dates, values, amp::Float64,
                              depth::Int=0, parent_start::Union{Date,Nothing}=nothing,
                              max_depth::Int=MAX_NESTING_DEPTH)
    """
    Recursively detect shrinking events at increasing depths.
    Uses different parameters for outer vs nested events.
    """
    
    id_val = String(id_val)
    dates = collect(dates)
    values = collect(values)
    
    all_events = DataFrame(
        id=String[],
        year=Int[],
        start=Date[],
        stop=Date[],
        recovery=Date[],
        shrink_days=Int[],
        total_days=Int[],
        start_doy=Int[],
        end_doy=Int[],
        recovery_doy=Int[],
        start_D_mean=Float64[],
        end_D_mean=Float64[],
        recovery_D_mean=Float64[],
        total_shrink=Float64[],
        shrink_threshold=Float64[],
        shrink_pct=Float64[],
        depth=Int[],
        event_type=String[],
        parent_start=Union{Date,Missing}[]
    )
    
    if depth >= max_depth
        return all_events
    end
    
    # Choose parameters based on depth
    println("Depth $depth: Detecting events for ID $id_val with $(length(values)) data points")
    min_shrink = depth > 0 ? NESTED_MIN_SHRINK_PCT : MIN_SHRINK_PCT
    recovery = depth > 0 ? NESTED_RECOVERY_PCT : RECOVERY_PCT
    
    # Detect events at current level with appropriate parameters
    events = detect_shrinking_events(dates, values, amp, 
                                     min_shrink_pct=min_shrink,
                                     recovery_pct=recovery)
    
    # Process each event
    for row in eachrow(events)
        event_type = if depth == 0
            "outer"
        elseif depth == 1
            "inner"
        else
            "inner_$(depth)"
        end
        
        # Add current event
        push!(all_events, (
            id=id_val,
            year=year(row.start_date),
            start=row.start_date,
            stop=row.end_date,
            recovery=row.recovery_date,
            shrink_days=row.duration,
            total_days=row.recovery_duration,
            start_doy=dayofyear(row.start_date),
            end_doy=dayofyear(row.end_date),
            recovery_doy=dayofyear(row.recovery_date),
            start_D_mean=row.start_val,
            end_D_mean=row.end_val,
            recovery_D_mean=row.recovery_val,
            total_shrink=row.shrinkage,
            shrink_threshold=row.shrink_threshold,
            shrink_pct=row.shrinkage_pct,
            depth=depth,
            event_type=event_type,
            parent_start=isnothing(parent_start) ? missing : parent_start
        ))
        
        # Look for nested events within the recovery period
        event_mask = (dates .>= row.end_date) .& (dates .<= row.recovery_date)
        event_dates = dates[event_mask]
        event_values = values[event_mask]
        
        # Only recurse if we have enough data points
        
            event_amp = maximum(event_values) - minimum(event_values)
            
            # Recursively detect nested events
            nested = detect_nested_events(
                id_val,
                event_dates,
                event_values,
                event_amp,
                depth + 1,
                row.start_date,
                max_depth
            )
            
            filtered_nested = filter(nested) do n
                n.start != row.start_date || n.stop != row.end_date
            end
            
            append!(all_events, filtered_nested)
        
    end
    
    return all_events
end

# Detect events for all IDs
println("\nDetecting shrinking events (max depth: $MAX_NESTING_DEPTH)...")
println("Outer events: MIN_SHRINK_PCT=$MIN_SHRINK_PCT%, RECOVERY_PCT=$RECOVERY_PCT%")
println("Nested events: MIN_SHRINK_PCT=$NESTED_MIN_SHRINK_PCT%, RECOVERY_PCT=$NESTED_RECOVERY_PCT%")

all_events = DataFrame(
    id=String[],
    year=Int[],
    start=Date[],
    stop=Date[],
    recovery=Date[],
    shrink_days=Int[],
    total_days=Int[],
    start_doy=Int[],
    end_doy=Int[],
    recovery_doy=Int[],
    start_D_mean=Float64[],
    end_D_mean=Float64[],
    recovery_D_mean=Float64[],
    total_shrink=Float64[],
    shrink_threshold=Float64[],
    shrink_pct=Float64[],
    depth=Int[],
    event_type=String[],
    parent_start=Union{Date,Missing}[]
)

for g in groupby(df, :id)
    id_val = first(g.id)
    dates = g.date
    values = g.D_mean
    amp = first(g.amp)
    
    events = detect_nested_events(id_val, dates, values, amp, 0, nothing, MAX_NESTING_DEPTH)
    append!(all_events, events)
end

println("\nDetection complete!")
println("Total events detected: $(nrow(all_events))")

if nrow(all_events) > 0
    sort!(all_events, [:id, :start, :depth])
    
    println("\nEvents by depth:")
    for depth_group in groupby(all_events, :depth)
        depth_val = first(depth_group.depth)
        event_type = first(depth_group.event_type)
        n_events = nrow(depth_group)
        mean_duration = round(mean(depth_group.shrink_days), digits=1)
        mean_shrink = round(mean(depth_group.shrink_pct), digits=1)
        
        println("  Depth $depth_val ($event_type): $n_events events")
        println("    Mean duration: $mean_duration days")
        println("    Mean shrinkage: $mean_shrink%")
    end
    
    println("\nOverall statistics:")
    println("Mean shrinking duration: $(round(mean(all_events.shrink_days), digits=1)) days")
    println("Mean total duration (to recovery): $(round(mean(all_events.total_days), digits=1)) days")
    println("Mean shrinkage: $(round(mean(all_events.shrink_pct), digits=1))% of amplitude")
    
    println("\nSample of detected events:")
    println(first(all_events, 15))
    
    # Save results
    CSV.write("input/shrinking-events.csv", all_events)
    println("\nResults saved to input/shrinking-events.csv")
else
    println("\nNo events detected.")
end
