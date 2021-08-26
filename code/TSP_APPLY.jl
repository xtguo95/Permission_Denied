using JuMP, GLPK
using DataFrames, CSV
using LinearAlgebra
using JSON
using LightGraphs

cd(@__DIR__)
include("TSP_FUNCTIONS.jl")

# Read Data Input from training stage

#route_opt_seq_df = DataFrame(CSV.File("../submission/data/build_route_with_seq.csv"));
route_opt_seq_df = DataFrame(CSV.File("../data/model_apply_outputs/model_apply_output/build_route_with_seq.csv"));
zone_travel_time_matrix = JSON.parsefile("../data/model_apply_outputs/model_apply_output/zone_mean_travel_times.json")
stop_travel_time_matrix = JSON.parsefile("../data/model_apply_inputs/new_travel_times.json");
zone_min_tt_nearest_df = DataFrame(CSV.File("../data/model_apply_outputs/model_apply_output/zone_min_tt_df_nearest_9.csv"));
zone_ecu_tt_nearest_df = DataFrame(CSV.File("../data/model_apply_outputs/model_apply_output/zone_ecu_dist_df_nearest_9.csv"));
route_id_list = unique(route_opt_seq_df.route_id);

β = 3.8; γ = 2.5; α = 1.04

# Calculate Zone sequence
Tour_TSP_zone_seq = Dict()
for route_id in route_id_list

    route_df = route_opt_seq_df[route_opt_seq_df.route_id .== route_id, :]
    zone_list = unique(route_df.zone_id)
    zone_distance_matrix = zone_travel_time_matrix[route_id]

    route_min_tt_nearest = zone_min_tt_nearest_df[zone_min_tt_nearest_df.route_id .== route_id, :]
    route_ecu_tt_nearest = zone_ecu_tt_nearest_df[zone_ecu_tt_nearest_df.route_id .== route_id, :]
    adj_zone_distance_matrix = adjust_zone_dist_matrix(zone_distance_matrix, zone_list, route_min_tt_nearest, route_ecu_tt_nearest, β, γ, α)

    tour_tsp_zone_seq = calculate_tsp_seq(route_df, adj_zone_distance_matrix)
    Tour_TSP_zone_seq[route_id] = tour_tsp_zone_seq
end

# Output best zone sequence
open("../data/model_apply_outputs/model_apply_output/mean_dist/opt_zone_seq_tour.json","w") do f
    JSON.print(f, Tour_TSP_zone_seq)
end

tsp_opt_seq = Dict()
for route_id in route_id_list

    route_df = route_opt_seq_df[route_opt_seq_df.route_id .== route_id, :]
    stop_distance_matrix = stop_travel_time_matrix[route_id]

    TOUR_TSP_zone_sequence = Tour_TSP_zone_seq[route_id]
    TOUR_TSP_path = generate_complete_seq_multiple(TOUR_TSP_zone_sequence, route_df, stop_distance_matrix)

    tsp_opt_seq[route_id] = TOUR_TSP_path
end

# Output best complete sequence
open("../data/model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour.json","w") do f
    JSON.print(f, tsp_opt_seq)
end

println("Successfully generate proposed sequences, into the post-processing stge")
