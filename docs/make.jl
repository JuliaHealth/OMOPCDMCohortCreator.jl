using OMOPCDMCohortCreator
using Documenter

makedocs(;
    modules = [OMOPCDMCohortCreator],
    authors = "Jacob Zelko (aka TheCedarPrince) <jacobszelko@gmail.com> and contributors",
    repo = "https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/blob/{commit}{path}#L{line}",
    sitename = "OMOPCDMCohortCreator.jl",
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://JuliaHealth.github.io/OMOPCDMCohortCreator.jl",
        assets = String[],
        edit_link = "dev",
	footer = "Created by [Jacob Zelko](https://jacobzelko.com) & [Georgia Tech Research Institute](https://www.gtri.gatech.edu). [License](https://github.com/JuliaHealth/OMOPCDMCohortCreator.jl/blob/main/LICENSE)"
    ),
    pages = [
        "Home" => "index.md",
        "Tutorials" => [
                        "tutorials.md",
                        "tutorials/beginner_tutorial.md",
                        "tutorials/r_tutorial.md"
                       ],
        "API" => "api.md",
        "Contributing" => "contributing.md"
    ],
)

deploydocs(;
    repo = "github.com/JuliaHealth/OMOPCDMCohortCreator.jl",
    push_preview = true,
    devbranch = "main",
)
