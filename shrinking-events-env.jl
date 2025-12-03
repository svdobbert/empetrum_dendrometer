using DataFrames
using CSV
using Dates
using Statistics

# Read data
df_events = CSV.read("input/shrinking-events.csv", DataFrame)
df = CSV.read("input/daily_data_with_trends.csv", DataFrame)

# Convert date column and sort
df.date = Date.(df.date)
sort!(df, [:id, :date])

# Add time-based columns
df.year = year.(df.date)
df.week = week.(df.date)
df.month = month.(df.date)
df.doy = dayofyear.(df.date)

# Drop rows with missing D_mean
df = df[.!ismissing.(df.D_mean), :]

function calculate_before_shrinking(df::DataFrame, ids::Vector, dates::Vector, 
                                   n::Int, env::Symbol, func::String="sum", 
                                   as_percentage::Bool=false)
    results = Vector{Union{Missing, Float64}}(undef, length(ids))
    
    for (i, (id_val, date)) in enumerate(zip(ids, dates))
        start_date = date - Day(n)
        end_date = date
        
        start_doy = dayofyear(start_date)
        end_doy = dayofyear(end_date)
        
        sub = df[(df.id .== id_val) .& 
                 (df.date .>= start_date) .& 
                 (df.date .< end_date), :]
        
        # Calculate the value for this specific window
        if func == "sum"
            if isempty(sub) || all(ismissing.(sub[!, env]))
                results[i] = missing
            else
                val = sum(skipmissing(sub[!, env]))
                
                if as_percentage
                    id_data = df[df.id .== id_val, :]
                    
                    if start_doy < end_doy
                        historical_data = id_data[(id_data.doy .>= start_doy) .& (id_data.doy .< end_doy), :]
                    else
                        historical_data = id_data[(id_data.doy .>= start_doy) .| (id_data.doy .< end_doy), :]
                    end
                    
                    if !isempty(historical_data)
                        year_sums = Float64[]
                        for year_group in groupby(historical_data, :year)
                            if !all(ismissing.(year_group[!, env]))
                                push!(year_sums, sum(skipmissing(year_group[!, env])))
                            end
                        end
                        
                        if !isempty(year_sums)
                            avg_val = mean(year_sums)
                            results[i] = avg_val == 0 ? missing : (val / avg_val) * 100
                        else
                            results[i] = missing
                        end
                    else
                        results[i] = missing
                    end
                else
                    results[i] = val
                end
            end
        elseif func == "mean"
            if isempty(sub) || all(ismissing.(sub[!, env]))
                results[i] = missing
            else
                val = mean(skipmissing(sub[!, env]))
                
                if as_percentage
                    id_data = df[df.id .== id_val, :]
                    
                    if start_doy < end_doy
                        historical_data = id_data[(id_data.doy .>= start_doy) .& (id_data.doy .< end_doy), :]
                    else
                        historical_data = id_data[(id_data.doy .>= start_doy) .| (id_data.doy .< end_doy), :]
                    end
                    
                    if !isempty(historical_data)
                        year_means = Float64[]
                        for year_group in groupby(historical_data, :year)
                            if !all(ismissing.(year_group[!, env]))
                                push!(year_means, mean(skipmissing(year_group[!, env])))
                            end
                        end
                        
                        if !isempty(year_means)
                            avg_val = mean(year_means)
                            results[i] = avg_val == 0 ? missing : (val / avg_val) * 100
                        else
                            results[i] = missing
                        end
                    else
                        results[i] = missing
                    end
                else
                    results[i] = val
                end
            end
        elseif func == "max"
            if isempty(sub) || all(ismissing.(sub[!, env]))
                results[i] = missing
            else
                val = maximum(skipmissing(sub[!, env]))
                
                if as_percentage
                    id_data = df[df.id .== id_val, :]
                    
                    if start_doy < end_doy
                        historical_data = id_data[(id_data.doy .>= start_doy) .& (id_data.doy .< end_doy), :]
                    else
                        historical_data = id_data[(id_data.doy .>= start_doy) .| (id_data.doy .< end_doy), :]
                    end
                    
                    if !isempty(historical_data)
                        year_maxs = Float64[]
                        for year_group in groupby(historical_data, :year)
                            if !all(ismissing.(year_group[!, env]))
                                push!(year_maxs, maximum(skipmissing(year_group[!, env])))
                            end
                        end
                        
                        if !isempty(year_maxs)
                            avg_val = mean(year_maxs)
                            results[i] = avg_val == 0 ? missing : (val / avg_val) * 100
                        else
                            results[i] = missing
                        end
                    else
                        results[i] = missing
                    end
                else
                    results[i] = val
                end
            end
        elseif func == "min"
            if isempty(sub) || all(ismissing.(sub[!, env]))
                results[i] = missing
            else
                val = minimum(skipmissing(sub[!, env]))
                
                if as_percentage
                    id_data = df[df.id .== id_val, :]
                    
                    if start_doy < end_doy
                        historical_data = id_data[(id_data.doy .>= start_doy) .& (id_data.doy .< end_doy), :]
                    else
                        historical_data = id_data[(id_data.doy .>= start_doy) .| (id_data.doy .< end_doy), :]
                    end
                    
                    if !isempty(historical_data)
                        year_mins = Float64[]
                        for year_group in groupby(historical_data, :year)
                            if !all(ismissing.(year_group[!, env]))
                                push!(year_mins, minimum(skipmissing(year_group[!, env])))
                            end
                        end
                        
                        if !isempty(year_mins)
                            avg_val = mean(year_mins)
                            results[i] = avg_val == 0 ? missing : (val / avg_val) * 100
                        else
                            results[i] = missing
                        end
                    else
                        results[i] = missing
                    end
                else
                    results[i] = val
                end
            end
        elseif func == "time_min"
            if isempty(sub) || all(ismissing.(sub[!, env]))
                results[i] = missing
            else
                valid_rows = .!ismissing.(sub[!, env])
                valid_sub = sub[valid_rows, :]
                min_idx = argmin(valid_sub[!, env])
                min_date = valid_sub.date[min_idx]
                results[i] = (end_date - min_date).value
            end
        else
            error("Invalid function: $func")
        end
    end
    
    println("$env $n $(as_percentage ? "(percentage)" : "")")
    return results
end

event_ids = Vector(df_events.id)
start_shrink = Date.(df_events.start)
stop_shrink = Date.(df_events.stop)
recovery = Date.(df_events.recovery)

# Build the shrinking dataframe
shrinking_df = DataFrame(
    id = df_events.id,
    year = df_events.year,
    start = df_events.start,
    stop = df_events.stop,
    recovery = df_events.recovery,
    shrink_days = df_events.shrink_days,
    total_days = df_events.total_days,
    start_doy = df_events.start_doy,
    end_doy = df_events.end_doy,
    recovery_doy = df_events.recovery_doy,
    total_shrink = df_events.total_shrink,
    shrink_pct = df_events.shrink_pct,
    depth = df_events.depth
)

env_vars = [:AT_mean, :ST_mean, :SM_mean]
env_names = ["AT", "ST", "SM"]


configurations = [
    (3, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (3, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (7, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (7, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (14, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (14, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (21, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (21, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (30, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (30, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (60, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (60, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (90, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (90, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (365, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (365, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true]),
    (1095, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [false]),
    (1095, ["sum", "mean", "max", "min", "time_min"], ["start", "stop", "recovery"], [true])
]

# Generate all columns
println("Generating calculated columns...")
for (n, funcs, timepoints, pct_options) in configurations
    for func in funcs
        for (env_idx, env) in enumerate(env_vars)
            env_name = env_names[env_idx]
            
            for timepoint in timepoints
                dates = if timepoint == "start"
                    start_shrink
                elseif timepoint == "stop"
                    stop_shrink
                elseif timepoint == "recovery"
                    recovery
                else
                    error("Unknown timepoint: $timepoint")
                end
                
                for use_pct in pct_options
                    col_name = if use_pct
                        "$(func)_$(env_name)_$(n)_$(timepoint)_pct"
                    else
                        "$(func)_$(env_name)_$(n)_$(timepoint)"
                    end
                    
                    col_symbol = Symbol(col_name)
                    shrinking_df[!, col_symbol] = calculate_before_shrinking(
                        df, event_ids, dates, n, env, func, use_pct
                    )
                end
            end
        end
    end
end

# save
println("\nFirst rows of result:")
println(first(shrinking_df, 5))
CSV.write("input/shrinking-events-env.csv", shrinking_df)
println("\nSaved to input/shrinking-events-env.csv")

# Create long format version
println("\nCreating long format...")
base_cols = [:id, :year, :start, :stop, :recovery, :shrink_days, :total_days, 
             :start_doy, :end_doy, :recovery_doy, :total_shrink, :shrink_pct, :depth]

long_data = DataFrame()
calc_cols = setdiff(names(shrinking_df), String.(base_cols))

for col in calc_cols
    global long_data
    
    parts = split(col, "_")
    is_pct = (length(parts) > 0 && parts[end] == "pct")
    
    if is_pct
        parts = parts[1:end-1]
    end
    
    if length(parts) >= 4
        func = parts[1]
        
        if func == "time" && parts[2] == "min"
            func = "time_min"
            env = parts[3]
            n = parts[4]
            timepoint = join(parts[5:end], "_")
        else
            env = parts[2]
            n = parts[3]
            timepoint = join(parts[4:end], "_")
        end
        
        try
            n_int = parse(Int, n)
            
            temp_df = select(shrinking_df, base_cols..., col => :value)
            temp_df.env = fill(env, nrow(temp_df))
            temp_df.n = fill(n_int, nrow(temp_df))
            temp_df.func = fill(func, nrow(temp_df))
            temp_df.timepoint = fill(timepoint, nrow(temp_df))
            temp_df.pct = fill(is_pct, nrow(temp_df))
            
            if isempty(long_data)
                long_data = temp_df
            else
                long_data = vcat(long_data, temp_df)
            end
        catch e
            println("Skipping column $col - could not parse: $e")
        end
    end
end

long_data = select(long_data, base_cols..., :env, :n, :func, :timepoint, :pct, :value)

println("\nLong format preview:")
println(first(long_data, 10))

CSV.write("input/shrinking-events-env-long.csv", long_data)
println("\nLong format saved to input/shrinking-events-env-long.csv")