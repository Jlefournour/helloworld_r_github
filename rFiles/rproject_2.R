# detach loaded libraries
detach("package:rsparkling", unload = TRUE)
if ("package:h2o" %in% search()) {detach("package:h2o", unload = TRUE)}
if (isNamespaceLoaded("h2o")) { unloadNamespace("h2o") }
# remove h2o from your installation
remove.packages("h2o", lib = .libPaths()[1])

# install last h2o for which sparkling is available
install.packages("h2o", type = "source",
                 repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-tverberg/2/R")

install.packages("h2o")
install.packages("rsparkling")