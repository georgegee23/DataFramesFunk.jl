module DataFramesFunk


########################################## DataFrame Manupulation Functions ###########################################################

using DataFrames
using ShiftedArrays
using DataFramesMeta


####### DataFrames MANIPULATIONS ##########################################################

function missing_to_nan(df::DataFrame)::DataFrame

    """

    Fill dataframe missing values with NaN.  

    """

    for col in names(df)
        df[!, col] = convert(Vector{Float64}, coalesce.(df[!, col], NaN))
    end

    return df
end

function category_dataframes(returns::DataFrame, categories::DataFrame)

    sector_dataframe_dict = Dict{String, DataFrame}()

    category_names = Set(categories[:, :Category])
    for cat_name in category_names

        selected_ids = @subset(categories, :Category .== cat_name)[!, "Security_ID"]
        category_rets_df = returns[:, selected_ids]

        sector_dataframe_dict[cat_name] = category_rets_df
    end
    return sector_dataframe_dict
end

function rowwise_ntiles(factors::DataFrame, n::Int)
    # Create a copy to avoid modifying the original DataFrame
    quantiles_df = deepcopy(factors)
    
    # Apply ntile function to each row
    for i in 1:size(factors,1)
        row = collect(factors[i, :])  # Convert row to vector for ntile function
        quantiles_df[i, :] = ntile(row, n)
    end
    
    return quantiles_df
end

function rowwise_percentiles(df::DataFrame)

    """ 
    
    Convert dataframe rows into rank percentile.

    """

    if isempty(df)
        return df  # Return an empty DataFrame if the input is empty
    end
    
    ntile_df = copy(df) # Create a copy to not modify the original DataFrame
    
    for (i, row) in enumerate(eachrow(ntile_df))
        # Convert each row to a vector, suitable for operations like ntile
        vector_row = collect(row)
        
        # Count non-missing values in the row
        n = count(!ismissing, vector_row)
        
        # Rank elements and divide by the number of non-missing elements,
        # which gives percentiles (1/n, 2/n, ..., n/n) for each non-missing element.
        ranks = sortperm(sortperm(filter(!ismissing, vector_row)))
        vector_row[.!ismissing.(vector_row)] .= (ranks ./ n)
        
        # Replace the row in ntile_df with the computed percentiles
        ntile_df[i, :] = vector_row
    end
    
    return ntile_df
end

function shift_dataframe(df::DataFrame; shift::Int, fill_value=missing)

    """
    Creates a new DataFrame where each column is shifted_df by the specified number of periods.

    # Arguments:
    - `df::DataFrame`: The input DataFrame to be shifted_df.
    - `lags::Int`: The number of periods to lag each column. Default is 1.
    - `fill_value`: The value to fill where data does not exist due to lagging (e.g., at the start of the series). Default is `missing`.

    # Returns:
    - A new DataFrame with shifted_df columns.

    """

    shifted_df = DataFrame()
    for col in names(df)
        shifted_df[!, col] = ShiftedArrays.lag(df[!, col], shift, default=fill_value)
    end
    return shifted_df
end

function dict_to_rowframe(dict, col_names)

    """

    Convert a dictionary to a DataFrame row-wise, where the keys are rows. 

    """

    df = DataFrame(collect(values(dict)), :auto)
    df = permutedims(df)
    df = rename(df, col_names)
    df[!, "Key"] = collect(keys(dict))
    df = sort(df, :Key)
    return df
end

function mask_dataframe(df::DataFrame, bool_df:: DataFrame)

    """

    Mask a dataframe with a boolean dataframe of the same size.

    """
    
    @assert size(df) == size(bool_df) "DataFrames must have the same dimensions"

    # Create a new DataFrame with selected values
    result = DataFrame([ifelse(bool_df[i,j], df[i,j], missing) for i in axes(df, 1), j in axes(df, 2)], names(df))
    return result
end

function rollmax_dataframe(df::DataFrame, window::Int)

    """

    Calculates the cumulative rolling maximum of each column in a DataFrame.

    **Arguments:**
    - `df::DataFrame`: The input DataFrame.
    - `window::Int`: The size of the rolling window.

    **Returns:**
    - `DataFrame`: A new DataFrame with the rolling maximum for each column.

    **Note:**
    - This function assumes that larger values are considered "maximum".
    - Missing values will be propagated; if you want to skip missings, ensure to clean your data beforehand or handle them within the function.
    
    """


    rollmax_df = DataFrame()
    for col in names(df)
        rollmax_df[!, col] = rollmax(df[!,col], window)
    end
    return rollmax_df
end

function rollstd_dataframe(df::DataFrame, window::Int)

    """

    Calculates the rolling standard deviation of each column in a DataFrame.

    **Arguments:**
    - `df::DataFrame`: The input DataFrame.
    - `window::Int`: The size of the rolling window.

    **Returns:**
    - `DataFrame`: A new DataFrame with the rolling standard deviation for each column.

    """

    roll_df = DataFrame()
    for col in names(df)
        roll_df[!, col] = rolling(std, df[!,col], window; padding=missing)
    end
    return roll_df
end

function row_average(df::DataFrame)

    """

    Compute row averages of a DataFrame. 

    """
    vectors = collect.(skipmissing.(eachrow(df)))
    r_means = [isempty(v) ? missing : mean(v) for v in vectors]
    return r_means

end


function percentage_change(df::DataFrame, window::Int = 1)


    """

    Calculate the percentage change for each column in a DataFrame over a specified window.

    # Arguments
    - `df::DataFrame`: The input DataFrame containing numerical data where percentage changes are to be calculated.
    - `window::Int=1`: The number of periods to look back for calculating the percentage change. 
    Default is 1, meaning the change is calculated from one period to the next.

    # Returns
    - `DataFrame`: A new DataFrame where each column represents the percentage change of the corresponding column in `df`.
    - The first `window` rows will be `missing` because there isn't enough data to calculate the percentage change.
    - If the previous value in the window is `missing` or zero, the result for that entry will also be `missing`.

    # Throws
    - `ArgumentError`: If `window` is less than 1.

    """


    if window < 1
        throw(ArgumentError("Window size must be at least 1"))
    end

    # Create a new DataFrame with the same structure but allowing missing
    result_df = DataFrame([Vector{Union{Missing, Float64}}(undef, size(df, 1)) for _ in 1:size(df, 2)], names(df))
    
    # Set the first `window` rows to missing
    result_df[1:window,:] .= missing
    
    for col in names(df)
        data = convert(Vector{Union{Missing, Float64}}, df[!, col])
        
        # Calculate windowed percentage change
        pct_change = map(window + 1:length(data)) do i
            if ismissing(data[i - window]) || data[i - window] == 0
                missing
            else
                (data[i] - data[i - window]) / data[i - window]
            end
        end
        
        # Place the calculated percentage changes into the DataFrame
        result_df[(window + 1):end, col] = pct_change
    end

    return result_df
end


function rowwise_zscore(dataframe::DataFrame)::DataFrame

    """

    Transforms each row of a DataFrame into z-scores, handling missing data.

    """
    # Assuming sp_riskadj_momentum_score is your DataFrame
    zscore_df = deepcopy(dataframe)
    for idx in 1:size(zscore_df, 1)
        row_vector = collect(eachrow(zscore_df)[idx])
    
        if !all(ismissing, row_vector)
            zscore_df[idx, :] = zscore_nonmissing(row_vector)
        else
            # If all values are missing, keep them as is
            continue
        end
    end

    return zscore_df
end


#----------------------------------------------------------------------------
#Utility functions

function zscore_nonmissing(values_vec::Vector{Union{T, Missing}} where T<:Union{Float64, Int})::Vector{Union{Missing, Float64}}

    """

    Compute the z-score for each element in the vector `values_vec`, handling missing values.

    This function:
    - Ignores missing values for calculation of mean and standard deviation.
    - Returns a vector of the same length as `values_vec` where non-missing values are transformed into z-scores.
    - Missing values in the input vector remain as missing in the output.

    # Arguments
    - `v::Vector{Any}` : Vector containing numeric values and potentially missing values.

    # Returns
    - `Vector{Union{Missing, Float64}}`: Vector where each non-missing value has been converted to its z-score.

    """

    # Filter out missing values
    non_missing = skipmissing(values_vec)
    
    # Convert to Vector for calculations
    nonmissing_vec = collect(non_missing)
    
    # Check if there are enough non-missing values to compute z-scores
    if length(nonmissing_vec) < 2
        error("Need at least two non-missing values to compute z-scores.")
    end
    
    # Compute mean and standard deviation
    μ = mean(nonmissing_vec)
    σ = std(nonmissing_vec)
    
    # Compute z-scores for all values including missing ones
    zscores_vec = map(values_vec) do x
        if ismissing(x)
            missing
        else
            (x - μ) / σ
        end
    end

    return zscores_vec
end


######################### THE END #####################################################################

end
