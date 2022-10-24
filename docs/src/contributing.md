# Contributing to OMOPCDMCohortCreator 😁

OMOPCDMCohortCreator is currently under heavy development as we push to a primary release version. 
We follow a workflow pattern that is directly inspired by the [development workflow guide](http://docs.juliaplots.org/latest/contributing/#Development-Workflow-1) found in [`Plots.jl`](https://github.com/JuliaPlots/Plots.jl).
The general workflow we expect contributors to adhere to is as follows:

## 1. Create an Issue about the Problem 📝

If you want to add functionality or to work on a bug you found, open an issue first.
That'll save you from doing work that we may not support for OMOPCDMCohortCreator.

## 2. Fork the repo to your account 🍴

## 3. Create a branch based on what you are developing 🌳

Before making a branch, make sure to check that you are even with master via the following commands within your fork:

```sh
git fetch origin
git checkout master
git merge --ff-only origin/master
```

> The `--ff-only` flag will "fast forward" to newer commits. It will not create new merge commits.

Then, go ahead and create a branch that you could edit with the changes you want to see.
This is done by going into the root and typing: `git branch -b [name of your branch]`

## 4. Test, code, and commit ✏️

Once you have a fork, it is useful to make sure the fork was successful.
To verify that everything is operational, let's test it.
The following procedure is as follows:

1. Go into the root of your fork:

`cd OMOPCDMCohortCreator`

2. Open your Julia REPL and type the following within the repo:

```julia-repl
julia> ]
(@v###) pkg> activate .
(@v###) pkg> test
```

This might take some time, but if the installation on your computer is successful, it should say all tests passed.

After making the changes you wanted to make, run the tests again to make sure you did not introduce any breaking changes.
If everything passed, we can continue on to the next step.
If not, it is the responsibility of the contributor to resolve any conflicts or failing tests.
Don't worry!
We're happy to help you resolve errors. 😄
If you are stuck, go ahead and continue with this tutorial.

The way we do this is in three steps:

1. Add the files you have added or changed via `git add` 

2. After adding the files, we need to say what you did to the files (i.e. commit the files). This can be accomplished thusly: `git commit -m "your message"` 

3. Finally, let's push these changes to GitHub using `git push --set-upstream origin [name of the branch you made]`

## 5. Submitting your changes to the main project ✅

Almost done! Go to your fork and there should be a section that asks you to make a pull request (PR) from your branch. This allows the maintainers of OMOPCDMCohortCreator to see if they can add your changes to the main project. If not, you can click the "New pull request" button.

Make sure the "base" branch is OMOPCDMCohortCreator `dev` and the "compare" branch is the branch on your fork. 
To your PR, add an informative title and description, and link your PR to relevant issues or discussions. 
Finally, click "Create pull request". 

You may get some questions about it, and possibly suggestions of how to make it ready to go into the main project. 
If you had test errors or problems, we are happy to help you. 
Then, if all goes according to plan, it gets merged... **Thanks for the contribution!!** 🎉 🎉 🎉

## Note on Adding Dependencies 📚

As a rule, we try to avoid having too many dependencies.
Therefore, we request that if you have a PR that adds a new dependency, please have opened an issue previously.

### Adding Core Dependencies 📒

If you are working on introducing a new core dependency, make sure to add that dependency to the main `Project.toml` for `OMOPCDMCohortCreator`.
To do this, follow these steps:

1. Enter the root of the `OMOPCDMCohortCreator` directory 

```sh
cd /path/to/OMOPCDMCohortCreator.jl
```

2. Activate the `OMOPCDMCohortCreator` environment and add the dependency:

```julia-repl
julia> ]
(@v###) pkg> activate .
(OMOPCDMCohortCreator) pkg> add [NAME OF DEPENDENCY]
```

### Adding Test Dependencies 📋

If you are  introducing a new test dependency, make sure to add that dependency to the `Project.toml` located in the `OMOPCDMCohortCreator` test directory.
To do this, follow these steps:

1. Enter the test directory inside of the `OMOPCDMCohortCreator` directory 

```sh
cd /path/to/OMOPCDMCohortCreator.jl/test/
```

2. Activate the `OMOPCDMCohortCreator` test environment and add the dependency:

```julia
julia> ]
(@v###) pkg> activate .
(test) pkg> add [NAME OF DEPENDENCY]
```
