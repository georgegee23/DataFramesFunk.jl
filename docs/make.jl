using DataFramesFunk
using Documenter

DocMeta.setdocmeta!(DataFramesFunk, :DocTestSetup, :(using DataFramesFunk); recursive=true)

makedocs(;
    modules=[DataFramesFunk],
    authors="georgeg <georgegi86@gmail.com> and contributors",
    sitename="DataFramesFunk.jl",
    format=Documenter.HTML(;
        canonical="https://georgegee23.github.io/DataFramesFunk.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/georgegee23/DataFramesFunk.jl",
    devbranch="master",
)
