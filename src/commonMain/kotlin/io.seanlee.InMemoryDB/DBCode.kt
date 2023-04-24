package io.seanlee.InMemoryDB

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.*

// Define the schema for a table
interface TableSchema {
    // Map the column name to its type
    val columns: Map<String, ColumnType>
}

// Define the types that a column can have
sealed class ColumnType
object StringType : ColumnType()
object NumberType : ColumnType()

// Define a class to represent a row
class Row(private val schema: TableSchema, private val data: MutableMap<String, Any>) {

    // Check that the keys of the data map match the keys of the schema's column map
    init {
        if (data.keys != schema.columns.keys) {
            throw IllegalArgumentException("Row does not match table schema")
        }
    }

    // Get the value of a column in the row
    fun get(column: String): Any? {
        return data[column]
    }

    // Set the value of a column in the row, validating that the new value has the correct type
    fun set(column: String, value: Any) {
        if (!schema.columns.containsKey(column)) {
            throw IllegalArgumentException("Column does not exist: $column")
        }
        val columnType = schema.columns[column]
        if (columnType is NumberType && value !is Number) {
            throw IllegalArgumentException("Value is not of type Number for column $column")
        } else if (columnType is StringType && value !is String) {
            throw IllegalArgumentException("Value is not of type String for column $column")
        }
        data[column] = value
    }
}

// Define a class to represent a table
class Table(private val schema: TableSchema) {
    // The data is stored as a mutable list of rows
    private val data = mutableListOf<Row>()

    // Changes to the table trigger this flow to emit a unit, allowing observers to know that the table has changed
    private val changesFlow = MutableSharedFlow<Row>()

    // Add a new row to the table, with data specified as a map of column name to value
    fun add(row: Map<String, Any>): Row {
        // Create a new row object with the schema and data
        val newRow = Row(schema, row.toMutableMap())
        // Add the new row to the data list
        data.add(newRow)
        // Emit a unit through the changes flow to signal that the table has changed
        changesFlow.tryEmit(newRow)
        // Return the new row object
        return newRow
    }

    // Remove rows from the table that match a given predicate function
    fun remove(predicate: (Row) -> Boolean): Table {
        // Use an iterator to iterate over the data list
        val iterator = data.iterator()
        while (iterator.hasNext()) {
            val row = iterator.next()
            // If the row matches the predicate, remove it from the data list and emit a unit through the changes flow
            if (predicate(row)) {
                iterator.remove()
                changesFlow.tryEmit(row)
            }
        }
        // Return the current table object (for method chaining)
        return this
    }

    // Edit rows in the table that match a given predicate function, updating columns with new values specified as a map of column name to value
    fun edit(predicate: (Row) -> Boolean, changes: Map<String, Any>): Table {
        // Find all rows that match the predicate and iterate over them
        data.filter(predicate).forEach { row ->
            // For each row, apply the changes to the columns specified in the changes map
            changes.forEach { (column, value) ->
                row.set(column, value) // Update the row with the new value
            }
            changesFlow.tryEmit(row) // Emit a change event to notify any listeners of the change
        }
        return this // Return the table instance to support chaining of method calls
    }

    // Return a list of maps representing the rows in the table
    fun view(): List<Map<String, Any>> {
        return data.map { row ->
            row.getValues() // Get the values for all columns in the row and return as a map
        }
    }


    // Filter the rows in the table based on a specified filter type and arguments, returning a new filtered table
    fun filter(type: String, vararg args: Any): Table {
        val filteredData = data.filter { row ->
            val column = args[0] as String
            val value = args[1]

            when (type) {
                "equalTo" -> row.get(column) == value
                "between" -> {
                    val start = value as Int
                    val end = args[2] as Int
                    val actual = row.get(column) as Int
                    actual in start..end
                }
                "startsWith" -> {
                    val prefix = value as String
                    val actual = row.get(column) as String
                    actual.startsWith(prefix)
                }
                else -> throw IllegalArgumentException("Invalid filter type: $type")
            }
        }

        // Create a new table instance with the same schema as the original table and add the filtered rows to it
        val filteredTable = Table(schema)
        filteredData.forEach { filteredTable.add(it.getValues()) }
        return filteredTable // Return the filtered table instance
    }

    // Return a flow of change events for the table
    fun changes(): Flow<Row> {
        return changesFlow.asSharedFlow()
    }

    // Get the values for all columns in the row and return as a map
    private fun Row.getValues(): Map<String, Any> {
        val values = mutableMapOf<String, Any>()
        schema.columns.keys.forEach { column ->
            // Get the value for the column, or throw an exception if it is not found
            values[column] = get(column) ?: throw IllegalStateException("Missing value for column: $column")
        }
        return values
    }
}

// Define a class to represent the database
class InMemoryDB {

    // Map to store all tables in the database, with the table name as the key and the Table object as the value
    private val tables = mutableMapOf<String, Table>()

    // Create a new table with the given name and schema and add it to the database
    fun createTable(name: String, schema: TableSchema): InMemoryDB {
        tables[name] = Table(schema)
        return this
    }

    // Get a reference to an existing table in the database by name
    fun getTable(name: String): Table {
        // If the table does not exist in the database, throw an exception
        return tables[name] ?: throw IllegalArgumentException("Table not found: $name")
    }

    // Observe changes to a table in the database by name, returning a flow that emits the table's current data and subsequent changes
    fun observeTable(name: String): Flow<List<Map<String, Any>>> {
        // Create a new shared flow to emit the table's data
        val sharedFlow = MutableSharedFlow<List<Map<String, Any>>>()
        // Get a reference to the table by name
        val table = getTable(name)
        // Emit the table's current data through the shared flow
        table.view().also { sharedFlow.tryEmit(it) }

        // Listen for changes to the table and emit them through the shared flow
        table.changes().onEach {
            sharedFlow.tryEmit(table.view())
        }.launchIn(GlobalScope)

        // Return the shared flow as a read-only flow
        return sharedFlow.asSharedFlow()
    }

    // Remove all tables from the database
    fun close() {
        tables.clear()
    }
}



