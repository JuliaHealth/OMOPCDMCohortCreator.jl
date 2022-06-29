using DBInterface
using LibPQ
using OMOPCDMCohortCreator
using OMOPCDMDatabaseConnector

conn = DBInterface.connect(LibPQ.Connection, "")

GenerateConnectionDetails(
    :postgresql,
    "synpuf5"
)

tables = GenerateTables(conn)

GetDatabasePersonIDs(conn)
