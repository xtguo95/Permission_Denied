using JuMP, GLPK
using DataFrames, CSV
using LinearAlgebra
using JSON
using LightGraphs

#cd("/home/xiaotong/Dropbox (MIT)/13_Amazon_LM_competition/submission_new")
cd(@__DIR__)

tsp_opt_seq = JSON.parsefile("../data/model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour_post_process.json")

# Output and store sequences
model_apply_output = Dict()
for i in keys(tsp_opt_seq)
    model_apply_output[i] = Dict()
    model_apply_output[i]["proposed"] = Dict()
    for j in 1:length(tsp_opt_seq[i])
        model_apply_output[i]["proposed"][tsp_opt_seq[i][j]] = j-1
    end
end

open("../data/model_apply_outputs/proposed_sequences.json","w") do f
    JSON.print(f, model_apply_output)
end

println("Successfully store proposed sequences")
