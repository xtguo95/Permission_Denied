"""
This script contains all functions used for TSP
"""

"Construct TSP pre-model, without cycle elimination mechanism"
function prebuild_tsp(dist::Matrix)

    n = size(dist, 1)
    # Definition of model
    model = Model(GLPK.Optimizer)
    set_optimizer_attribute(model, "tm_lim", 300 * 1_000)
    #model = Model(Gurobi.Optimizer)
    #set_optimizer_attributes(model, "OutputFlag" => 0)

    # Main variable: x_ij=1 if the tour visits i and j in that order, 0 otherwise
    @variable(model, x[1:n, 1:n], Bin)
    # Objective: minimizing the total cost (distance) of the tour
    @objective(model, Min, sum(dist[i, j] * x[i, j] for i = 1:n, j = 1:n))
    # SHARED CONSTRAINTS
    @constraint(
        model, no_self_edges[i = 1:n], x[i,i] == 0
    )
    @constraint(
        model, exactly_one_successor[i = 1:n], sum(x[i, j] for j = 1:n) == 1
    )
    @constraint(
        model, exactly_one_predecessor[j = 1:n], sum(x[i, j] for i = 1:n) == 1
    )
    return model, x
end

"""
    Given the induced graph as an adjacency list (i.e., next[i] is the next node to visit after node i),
        compute all subtours.
    Return them as a list of lists of nodes in the same component
"""
function find_subtours(next::Vector{Int})
    n = length(next)
    g = DiGraph(n)
    for i = 1:n
        add_edge!(g, i, next[i])
    end
    components = strongly_connected_components(g)
    return sort(components, by=length)
end

"Solve the TSP using an iterative approach"
function solve_iterative(dist_matrix::Dict, zone_list::Array; lower_bound=0, time_limit_seconds::Real = 600,
                         eliminate_length_2::Bool=false,
                         verbose::Bool = true)
    # We first solve the model without any subtour elimination consideration

    dist = zeros(length(zone_list), length(zone_list))
    for i in 1:length(zone_list), j in 1:length(zone_list)
        if i != j
            dist[i, j] = dist_matrix[zone_list[i]][zone_list[j]]
        end
    end

    model, x = prebuild_tsp(dist)
    n = size(dist,1)
    @constraint(model, sum(dist[i, j] * x[i, j] for i = 1:n, j = 1:n) >= lower_bound)
    if eliminate_length_2
        @constraint(model, no_length_2[i = 1:n, j = 1:n], x[i, j] + x[j, i] <= 1)
    end
    verbose || set_optimizer_attribute(model, "OutputFlag", 0)
    start=time()
    optimize!(model)

    while true
        # We store the incumbent solution
        next = [findfirst(x -> x > 0.5, value.(x[i, :])) for i = 1:n]
        # Note: checking for >0.5 is conservative (x is binary!) but it avoids numerical errors
        subtours = find_subtours(next)
        #println("Found $(length(subtours)) subtours after $(time() - start) seconds")
        if length(subtours) == 1 # only one cycle, the TSP solution
            solvetime = time() - start
            return solvetime, objective_value(model), value.(x)
        else
            # eliminate subtours
            for subtour in subtours
                @constraint(model, sum(x[i, j] for i=subtour, j=setdiff(1:n, subtour)) >= 1)
                @constraint(model, sum(x[i, j] for i=setdiff(1:n, subtour), j=subtour) >= 1)
            end
        end
        optimize!(model)
        time() - start > time_limit_seconds && return solvetime, objective_value(model), value.(x)
    end
end

function calculate_tsp_seq(route_df, zone_distance_matrix; first_zone = "INIT")

    zone_list = unique(skipmissing(route_df.zone_id))
    opt_zone_seq = zone_list
    compute_time, obj, opt_x = solve_iterative(zone_distance_matrix, zone_list)
    opt_ind = find_subtours([findfirst(x -> x > 0.5, opt_x[i, :]) for i = 1:size(opt_x,1)])[1]
    init_output_seq = [zone_list[i] for i in opt_ind]
    init_ind = findfirst(x -> x == first_zone,init_output_seq)
    zone_output_seq = vcat(init_output_seq[init_ind:end],init_output_seq[1:init_ind-1])

    return zone_output_seq
end

"Construct path TSP pre-model, without cycle elimination mechanism"
function prebuild_path_tsp(dist::Matrix, s, t)

    n = size(dist, 1)
    other_node_list = setdiff(collect(1:n), [s, t])
    # Definition of model
    model = Model(GLPK.Optimizer)
    set_optimizer_attribute(model, "tm_lim", 300 * 1_000)
    #model = Model(Gurobi.Optimizer)
    #set_optimizer_attributes(model, "OutputFlag" => 0)

    # Main variable: x_ij=1 if the tour visits i and j in that order, 0 otherwise
    @variable(model, x[1:n, 1:n], Bin)
    # Objective: minimizing the total cost (distance) of the tour
    @objective(model, Min, sum(dist[i, j] * x[i, j] for i = 1:n, j = 1:n))
    # SHARED CONSTRAINTS
    @constraint(
        model, no_self_edges[i = 1:n], x[i,i] == 0
    )
    @constraint(
        model, exactly_one_successor[i in other_node_list], sum(x[i, j] for j = 1:n) == 1
    )
    @constraint(model, sum(x[s, j] for j in 1:n) == 1)
    @constraint(model, sum(x[j, s] for j in 1:n) == 0)
    @constraint(model, sum(x[i, t] for i in 1:n) == 1)
    @constraint(model, sum(x[t, i] for i in 1:n) == 0)
    @constraint(
        model, exactly_one_predecessor[j in other_node_list], sum(x[i, j] for i = 1:n) == 1
    )
    return model, x
end

"""
    Given the induced graph as an adjacency list (i.e., next[i] is the next node to visit after node i),
        compute all subtours.
    Return them as a list of lists of nodes in the same component
"""
function find_path_subtours(next)
    n = length(next)
    g = DiGraph(n)
    for i = 1:n
        if next[i] != nothing
            add_edge!(g, i, next[i])
        end
    end
    cycles = simplecycles(g)
    #components = connected_components(g)
    return cycles #sort(components, by=length)
end

"Solve the TSP using an iterative approach"
function solve_path_iterative(dist_matrix::Dict, zone_list::Array, first_stop, last_stop; time_limit_seconds::Real = 600,
                         eliminate_length_2::Bool=false)
    # We first solve the model without any subtour elimination consideration

    dist = zeros(length(zone_list), length(zone_list))
    for i in 1:length(zone_list), j in 1:length(zone_list)
        if i != j
            dist[i, j] = dist_matrix[zone_list[i]][zone_list[j]]
        end
    end

    s = findfirst(x -> x == first_stop, zone_list)
    t = findfirst(x -> x == last_stop, zone_list)

    model, x = prebuild_path_tsp(dist, s, t)
    n = size(dist,1)
    if eliminate_length_2
        @constraint(model, no_length_2[i = 1:n, j = 1:n], x[i, j] + x[j, i] <= 1)
    end
    start=time()
    optimize!(model)


    while true
        # We store the incumbent solution
        next = [findfirst(x -> x > 0.5, value.(x[i, :])) for i = 1:n]
        # Note: checking for >0.5 is conservative (x is binary!) but it avoids numerical errors
        subtours = find_path_subtours(next)
        #println("Found $(length(subtours)) subtours after $(time() - start) seconds")
        if length(subtours) == 0 # no cycles
            solvetime = time() - start
            return solvetime, objective_value(model), value.(x), s
        else
            # eliminate subtours
            for subtour in subtours
                @constraint(model, sum(x[i, j] for i=subtour, j=setdiff(1:n, subtour)) >= 1)
                @constraint(model, sum(x[i, j] for i=setdiff(1:n, subtour), j=subtour) >= 1)
            end
        end
        optimize!(model)
        time() - start > time_limit_seconds && return solvetime, objective_value(model), value.(x), s
    end
end

function calculate_path_tsp_seq(route_df, first_zone, last_zone, zone_distance_matrix)

    zone_list = unique(skipmissing(route_df.zone_id))
    if first_zone != "INIT" && last_zone != "INIT"
        zone_list = zone_list[2:end]
    end

    adj_zone_distance_matrix = Dict()
    for i in zone_list
        adj_zone_distance_matrix[i] = Dict()
        for j in zone_list
            adj_zone_distance_matrix[i][j] = 0
        end
    end

    for i in zone_list
        for j in zone_list
            adj_zone_distance_matrix[i][j] = zone_distance_matrix[i][j]
        end
    end

    compute_time, obj, opt_x = solve_path_iterative(adj_zone_distance_matrix, zone_list, first_zone, last_zone)


    next = [findfirst(x -> x > 0.5, opt_x[i, :]) for i = 1:size(opt_x,1)]

    output_seq = [first_zone]
    while length(output_seq) < length(zone_list)
        next_zone_ind = next[findfirst(x -> x == output_seq[end], zone_list)]
        next_zone = zone_list[next_zone_ind]
        push!(output_seq, next_zone)
    end

    @assert output_seq[1] == first_zone
    @assert output_seq[end] == last_zone

    return output_seq
end

function find_first_and_last_stop_list(prev_node, zone, next_zone, zone_to_node_dict, stop_distance_matrix)

    zone_node = zone_to_node_dict[zone]
    next_zone_node = zone_to_node_dict[next_zone]

    prev_dist_list = []
    next_dist_list = []
    for node in zone_node

        prev_dist = stop_distance_matrix[prev_node][node]
        push!(prev_dist_list, (prev_dist, node))

        next_dist = 0
        for next_node in next_zone_node
            next_dist += stop_distance_matrix[node][next_node]
        end
        push!(next_dist_list, (next_dist, node))
    end

    first_node_list = [i[2] for i in sort(prev_dist_list)[1:min(length(zone_node), 3)]]
    last_node_list = [i[2] for i in sort(next_dist_list)[1:min(length(zone_node), 3)]]

    return first_node_list, last_node_list
end

function generate_complete_seq_multiple(zone_seq, route_df, stop_distance_matrix)
    zone_output_seq = zone_seq # opt_zone_seq

    # Generate zone-node correspondence dict
    zone_to_node_dict = Dict(i => [] for i in zone_seq)
    selected_df = route_df[:, ["stops", "zone_id"]]
    for i in 1:size(selected_df, 1)
        stop_id = selected_df[i,"stops"]
        zone_id = selected_df[i,"zone_id"]
        push!(zone_to_node_dict[zone_id], stop_id)
    end

    init_stop = zone_to_node_dict["INIT"][1]
    output_complete_sequence = [init_stop]
    prev_node = init_stop
    for i in 2:length(zone_output_seq)
        curr_zone = zone_output_seq[i]
        # Zone only contains one stop
        if length(zone_to_node_dict[curr_zone]) <= 1
            push!(output_complete_sequence, zone_to_node_dict[curr_zone][1])
            continue
        end
        if i == length(zone_output_seq)
            next_zone = zone_output_seq[1]
        else
            next_zone = zone_output_seq[i+1]
        end
        first_node_list, last_node_list = find_first_and_last_stop_list(prev_node, curr_zone, next_zone, zone_to_node_dict, stop_distance_matrix)
        stop_list = zone_to_node_dict[curr_zone]

        inner_option_list = []
        for first_node in first_node_list
            for last_node in last_node_list
                if first_node == last_node
                    compute_time, obj, opt_x = solve_iterative(stop_distance_matrix, stop_list)
                    opt_ind = find_subtours([findfirst(x -> x > 0.5, opt_x[i, :]) for i = 1:size(opt_x,1)])[1]
                    init_output_seq = [stop_list[i] for i in opt_ind]
                    init_ind = findfirst(x -> x == first_node,init_output_seq)
                    output_path = vcat(init_output_seq[init_ind:end],init_output_seq[1:init_ind-1])
                    reduced_obj = obj - stop_distance_matrix[output_path[end]][output_path[1]]

                    push!(inner_option_list, (reduced_obj, first_node, last_node, output_path))
                else
                    compute_time, obj, opt_x, s = solve_path_iterative(stop_distance_matrix, stop_list, first_node, last_node);
                    next = [findfirst(x -> x > 0.5, opt_x[i, :]) for i = 1:size(opt_x,1)]
                    output_seq = [s]
                    while length(output_seq) < length(stop_list)
                        next_visit_node = next[output_seq[end]]
                        push!(output_seq, next_visit_node)
                    end
                    output_path = [stop_list[i] for i in output_seq]

                    @assert first_node == output_path[1]
                    @assert last_node == output_path[end]

                    push!(inner_option_list, (obj, first_node, last_node, output_path)) # output_path
                end
            end
        end

        inner_zone_tsp_path = sort(inner_option_list)[1][4]
        first_node = inner_zone_tsp_path[1]
        last_node = inner_zone_tsp_path[end]

        output_complete_sequence = vcat(output_complete_sequence, inner_zone_tsp_path)

        prev_node = last_node
    end

    return output_complete_sequence
end

function adjust_zone_dist_matrix(zone_distance_matrix, zone_list, route_min_tt_nearest, route_ecu_tt_nearest, β, γ, α)

    adj_zone_dist_matrix = Dict(i => Dict() for i in zone_list)

    for i in zone_list, j in zone_list

        try
            global big_zone_i = split(i, ".")[1]
            global big_zone_j = split(j, ".")[1]
        catch
            global big_zone_i = i
            global big_zone_j = j
        end

        if big_zone_i != "INIT" && big_zone_j != "INIT" && big_zone_i != big_zone_j
            adj_zone_dist_matrix[i][j] = zone_distance_matrix[i][j] * β
        else
            adj_zone_dist_matrix[i][j] = zone_distance_matrix[i][j]
        end

        if i == "INIT"
            if j ∉ route_ecu_tt_nearest[route_ecu_tt_nearest.from_zone .== i, :].to_zone && j ∉ route_min_tt_nearest[route_min_tt_nearest.from_zone .== i, :].to_zone
                adj_zone_dist_matrix[i][j] = zone_distance_matrix[i][j] * α
            end
        end

        # if i == "INIT" && j ∉ route_zone_min_tt_df[route_zone_min_tt_df.from_zone .== i, :].to_zone
        #     adj_zone_dist_matrix[i][j] = zone_distance_matrix[i][j] * α
        # end

        if i == "INIT" || j == "INIT"
            continue
        end

        try
            global after_dot_i1 = split(i, ".")[2][1]
            global after_dot_i2 = split(i, ".")[2][2]
            global after_dot_j1 = split(j, ".")[2][1]
            global after_dot_j2 = split(j, ".")[2][2]
            global first_diff = abs(Int(after_dot_i1) - Int(after_dot_j1))
            global second_diff = abs(Int(after_dot_i2) - Int(after_dot_j2))
            global sum_diff = first_diff + second_diff
        catch
            global sum_diff = 0
        end

        if big_zone_i == big_zone_j && sum_diff != 1
            adj_zone_dist_matrix[i][j] = adj_zone_dist_matrix[i][j] * γ
        else
            adj_zone_dist_matrix[i][j] = adj_zone_dist_matrix[i][j]
        end
    end

    return adj_zone_dist_matrix
end
