using System.Text.Json;
using System.IO.Compression;
using Microsoft.Data.Sqlite;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace BibleDbConverter;

public class Program
{
    private static ILogger<Program>? _logger;

    public static async Task Main(string[] args)
    {
        // Setup logging
        using var loggerFactory = LoggerFactory.Create(builder =>
            builder.AddConsole().SetMinimumLevel(LogLevel.Information));
        _logger = loggerFactory.CreateLogger<Program>();

        try
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: BibleDbConverter <json-directory> <output-directory>");
                Console.WriteLine("Example: BibleDbConverter ./BibleJson ./Output");
                return;
            }

            string jsonDirectory = args[0];
            string outputDirectory = args[1];

            if (!Directory.Exists(jsonDirectory))
            {
                Console.WriteLine($"JSON directory not found: {jsonDirectory}");
                return;
            }

            Directory.CreateDirectory(outputDirectory);

            var converter = new BibleDbConverter(_logger);
            await converter.ConvertAsync(jsonDirectory, outputDirectory);

            Console.WriteLine("\n✅ Conversion completed successfully!");
            Console.WriteLine($"📁 Output files saved to: {outputDirectory}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error: {ex.Message}");
            _logger?.LogError(ex, "Conversion failed");
        }
    }
}

public class BibleDbConverter
{
    private readonly ILogger _logger;

    public BibleDbConverter(ILogger logger)
    {
        _logger = logger;
    }

    public async Task ConvertAsync(string jsonDirectory, string outputDirectory)
    {
        var tempDbPath = Path.Combine(outputDirectory, "bible_temp.db");
        var finalDbPath = Path.Combine(outputDirectory, "bible.db");
        var compressedDbPath = Path.Combine(outputDirectory, "bible.db.gz");

        try
        {
            _logger.LogInformation("🚀 Starting Bible database conversion...");

            // Clean up any existing temp files first
            await CleanupTempFilesAsync(tempDbPath, finalDbPath);

            // Step 1: Convert JSON to SQLite
            _logger.LogInformation("📖 Converting JSON files to SQLite database...");
            await ConvertJsonToSqliteAsync(jsonDirectory, tempDbPath);

            // Step 2: Optimize database (separate connection)
            _logger.LogInformation("⚡ Optimizing database structure...");
            await OptimizeDatabaseAsync(tempDbPath);

            // Step 3: Move optimized database
            if (File.Exists(finalDbPath))
                File.Delete(finalDbPath);

            File.Move(tempDbPath, finalDbPath);

            // Step 4: Create compressed version
            _logger.LogInformation("🗜️ Creating compressed database...");
            await CompressDatabaseAsync(finalDbPath, compressedDbPath);

            // Step 5: Display statistics
            await DisplayStatisticsAsync(jsonDirectory, finalDbPath, compressedDbPath);

            _logger.LogInformation("✨ Database conversion completed successfully!");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Conversion failed");
            throw;
        }
        finally
        {
            // Cleanup temp files with retry logic
            await CleanupTempFilesAsync(tempDbPath);
        }
    }

    private async Task CleanupTempFilesAsync(params string[] filePaths)
    {
        foreach (var filePath in filePaths)
        {
            if (!File.Exists(filePath)) continue;

            // Try to delete with retry logic
            for (int i = 0; i < 5; i++) // Increased retry count
            {
                try
                {
                    File.Delete(filePath);
                    break; // Success
                }
                catch (IOException) when (i < 4)
                {
                    // Wait and retry - give more time for file handles to release
                    await Task.Delay(500 * (i + 1)); // Exponential backoff

                    // Force garbage collection to release any lingering file handles
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
                catch (IOException ex)
                {
                    _logger.LogWarning(ex, "Could not delete temp file: {FilePath}", filePath);
                    if (i == 4) // Last attempt failed
                        throw;
                }
            }
        }
    }

    private async Task ConvertJsonToSqliteAsync(string jsonDirectory, string dbPath)
    {
        // Ensure the directory exists
        Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);

        // Create and open connection
        using var connection = new SqliteConnection($"Data Source={dbPath};Pooling=false"); // Disable pooling
        await connection.OpenAsync();

        try
        {
            // Create database schema
            await CreateDatabaseSchemaAsync(connection);

            // Get JSON files and sort them properly
            var jsonFiles = Directory.GetFiles(jsonDirectory, "*.json")
                .Where(f => !Path.GetFileName(f).StartsWith('.'))
                .OrderBy(Path.GetFileName)
                .ToList();

            _logger.LogInformation($"📚 Found {jsonFiles.Count} JSON files to process");

            // Insert default translation
            var translationId = await InsertTranslationAsync(connection, "KJV", "King James Version", "en");

            var bookOrder = 1;
            var totalVerses = 0;
            var totalBooks = 0;

            foreach (var jsonFile in jsonFiles)
            {
                var fileName = Path.GetFileNameWithoutExtension(jsonFile);
                _logger.LogInformation($"📄 Processing file: {fileName}...");

                // Process each file in its own transaction
                using var transaction = await connection.BeginTransactionAsync();

                try
                {
                    var versesProcessed = await ProcessBookFileAsync(connection, transaction, jsonFile, translationId, bookOrder);
                    totalVerses += versesProcessed;

                    await transaction.CommitAsync();

                    // Update bookOrder based on number of books processed in this file
                    var json = await File.ReadAllTextAsync(jsonFile);
                    var books = JsonSerializer.Deserialize<List<BibleBook>>(json, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    if (books?.Any() == true)
                    {
                        totalBooks += books.Count;
                        bookOrder += books.Count;
                    }
                }
                catch
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }

            // Build full-text search index
            _logger.LogInformation("🔍 Building full-text search index...");
            await BuildFullTextSearchAsync(connection, translationId);

            _logger.LogInformation($"📊 Processed {totalBooks} books from {jsonFiles.Count} files with {totalVerses:N0} verses");
        }
        finally
        {
            // Explicitly close and dispose
            await connection.CloseAsync();
            await connection.DisposeAsync();

            // Force cleanup
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private async Task CreateDatabaseSchemaAsync(SqliteConnection connection)
    {
        const string schema = @"
            -- Books table
            CREATE TABLE Books (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL COLLATE NOCASE,
                Testament INTEGER NOT NULL, -- 1=OT, 2=NT
                OrderNum INTEGER NOT NULL,
                ChapterCount INTEGER NOT NULL DEFAULT 0
            );
            
            -- Verses table
            CREATE TABLE Verses (
                Id INTEGER PRIMARY KEY,
                BookId INTEGER NOT NULL,
                Chapter INTEGER NOT NULL,
                Verse INTEGER NOT NULL,
                FOREIGN KEY (BookId) REFERENCES Books(Id)
            );
            
            -- Translations table
            CREATE TABLE Translations (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL,
                Language TEXT NOT NULL DEFAULT 'en'
            );
            
            -- Verse text table (optimized for storage)
            CREATE TABLE VerseText (
                VerseId INTEGER NOT NULL,
                TranslationId INTEGER NOT NULL,
                Text TEXT NOT NULL,
                PRIMARY KEY (VerseId, TranslationId),
                FOREIGN KEY (VerseId) REFERENCES Verses(Id),
                FOREIGN KEY (TranslationId) REFERENCES Translations(Id)
            ) WITHOUT ROWID;
            
            -- Indexes for performance
            CREATE INDEX idx_verse_lookup ON Verses(BookId, Chapter, Verse);
            CREATE INDEX idx_verse_text ON VerseText(TranslationId, VerseId);
            CREATE INDEX idx_book_order ON Books(OrderNum);
            
            -- Full-text search virtual table
            CREATE VIRTUAL TABLE VerseSearch USING fts5(
                reference UNINDEXED,
                text,
                translation UNINDEXED,
                content=''
            );";

        await connection.ExecuteAsync(schema);
    }

    private async Task<int> InsertTranslationAsync(SqliteConnection connection, string code, string name, string language)
    {
        const string sql = @"
            INSERT INTO Translations (Code, Name, Language) 
            VALUES (@code, @name, @language);
            SELECT last_insert_rowid();";

        return await connection.QuerySingleAsync<int>(sql, new { code, name, language });
    }

    private async Task<int> ProcessBookFileAsync(SqliteConnection connection, IDbTransaction transaction, string jsonFile, int translationId, int bookOrder)
    {
        var json = await File.ReadAllTextAsync(jsonFile);
        var books = JsonSerializer.Deserialize<List<BibleBook>>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        if (books == null || !books.Any())
        {
            _logger.LogWarning($"⚠️ Invalid or empty JSON file: {jsonFile}");
            return 0;
        }

        var totalVersesProcessed = 0;

        // Process each book in the JSON file
        foreach (var book in books)
        {
            if (book?.lstChapters == null)
            {
                _logger.LogWarning($"⚠️ Invalid book data in file: {jsonFile}");
                continue;
            }

            // Insert book
            const string insertBookSql = @"
            INSERT INTO Books (Name, Testament, OrderNum, ChapterCount) 
            VALUES (@name, @testament, @order, @chapterCount);
            SELECT last_insert_rowid();";

            var testament = DetermineTestament(book.BookName);
            var chapterCount = book.lstChapters.Count;

            var bookId = await connection.QuerySingleAsync<int>(insertBookSql, new
            {
                name = book.BookName,
                testament = testament,
                order = bookOrder,
                chapterCount = chapterCount
            }, transaction);

            var versesProcessed = 0;

            // Process all chapters and verses for this book
            foreach (var chapter in book.lstChapters.Where(c => c.Verses?.Count > 0))
            {
                foreach (var verse in chapter.Verses)
                {
                    // Insert verse
                    const string insertVerseSql = @"
                    INSERT INTO Verses (BookId, Chapter, Verse) 
                    VALUES (@bookId, @chapter, @verse);
                    SELECT last_insert_rowid();";

                    var verseId = await connection.QuerySingleAsync<int>(insertVerseSql, new
                    {
                        bookId = bookId,
                        chapter = chapter.ChapterNumber,
                        verse = verse.VerseNumber
                    }, transaction);

                    // Insert verse text
                    const string insertTextSql = @"
                    INSERT INTO VerseText (VerseId, TranslationId, Text) 
                    VALUES (@verseId, @translationId, @text)";

                    await connection.ExecuteAsync(insertTextSql, new
                    {
                        verseId = verseId,
                        translationId = translationId,
                        text = verse.ChapterVerse?.Trim() ?? ""
                    }, transaction);

                    versesProcessed++;
                }
            }

            _logger.LogInformation($"📖 Processed {book.BookName}: {versesProcessed:N0} verses");
            totalVersesProcessed += versesProcessed;
            bookOrder++; // Increment for each book in the array
        }

        return totalVersesProcessed;
    }

    private async Task BuildFullTextSearchAsync(SqliteConnection connection, int translationId)
    {
        const string buildFtsSql = @"
            INSERT INTO VerseSearch(reference, text, translation)
            SELECT 
                b.Name || ' ' || v.Chapter || ':' || v.Verse as reference,
                vt.Text as text,
                t.Code as translation
            FROM VerseText vt
            JOIN Verses v ON vt.VerseId = v.Id
            JOIN Books b ON v.BookId = b.Id
            JOIN Translations t ON vt.TranslationId = t.Id
            WHERE vt.TranslationId = @translationId";

        await connection.ExecuteAsync(buildFtsSql, new { translationId });
    }

    private async Task OptimizeDatabaseAsync(string dbPath)
    {
        // Use separate connection for optimization with no pooling
        using var connection = new SqliteConnection($"Data Source={dbPath};Pooling=false");
        await connection.OpenAsync();

        try
        {
            // Use basic ADO.NET commands instead of Dapper to avoid the CommandType issue
            using var vacuumCmd = connection.CreateCommand();
            vacuumCmd.CommandText = "VACUUM";
            await vacuumCmd.ExecuteNonQueryAsync();

            using var analyzeCmd = connection.CreateCommand();
            analyzeCmd.CommandText = "ANALYZE";
            await analyzeCmd.ExecuteNonQueryAsync();

            using var optimizeCmd = connection.CreateCommand();
            optimizeCmd.CommandText = "PRAGMA optimize";
            await optimizeCmd.ExecuteNonQueryAsync();

            _logger.LogInformation("✅ Database optimization completed");
        }
        finally
        {
            await connection.CloseAsync();
            await connection.DisposeAsync();

            // Force cleanup
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private async Task CompressDatabaseAsync(string sourcePath, string compressedPath)
    {
        if (File.Exists(compressedPath))
            File.Delete(compressedPath);

        using var sourceStream = File.OpenRead(sourcePath);
        using var compressedStream = File.Create(compressedPath);
        using var gzipStream = new GZipStream(compressedStream, CompressionLevel.SmallestSize);

        await sourceStream.CopyToAsync(gzipStream);

        var originalSize = new FileInfo(sourcePath).Length;
        var compressedSize = new FileInfo(compressedPath).Length;
        var compressionRatio = (1.0 - (double)compressedSize / originalSize) * 100;

        _logger.LogInformation($"💾 Compression: {FormatFileSize(originalSize)} → {FormatFileSize(compressedSize)} ({compressionRatio:F1}% reduction)");
    }

    private async Task DisplayStatisticsAsync(string jsonDirectory, string dbPath, string compressedPath)
    {
        using var connection = new SqliteConnection($"Data Source={dbPath};Pooling=false");
        await connection.OpenAsync();

        try
        {
            var stats = await connection.QuerySingleAsync(@"
                SELECT 
                    (SELECT COUNT(*) FROM Books) as BookCount,
                    (SELECT COUNT(*) FROM Verses) as VerseCount,
                    (SELECT COUNT(*) FROM Translations) as TranslationCount,
                    (SELECT COUNT(*) FROM VerseSearch) as SearchIndexSize");

            var jsonSize = Directory.GetFiles(jsonDirectory, "*.json")
                .Sum(f => new FileInfo(f).Length);

            var dbSize = new FileInfo(dbPath).Length;
            var compressedSize = new FileInfo(compressedPath).Length;

            Console.WriteLine("\n📈 CONVERSION STATISTICS");
            Console.WriteLine("================================");
            Console.WriteLine($"📚 Books:           {stats.BookCount:N0}");
            Console.WriteLine($"📝 Verses:          {stats.VerseCount:N0}");
            Console.WriteLine($"🌐 Translations:    {stats.TranslationCount:N0}");
            Console.WriteLine($"🔍 Search entries:  {stats.SearchIndexSize:N0}");
            Console.WriteLine();
            Console.WriteLine("💾 FILE SIZES");
            Console.WriteLine("================================");
            Console.WriteLine($"📄 Original JSON:   {FormatFileSize(jsonSize)}");
            Console.WriteLine($"🗃️ SQLite DB:       {FormatFileSize(dbSize)}");
            Console.WriteLine($"🗜️ Compressed:      {FormatFileSize(compressedSize)}");
            Console.WriteLine();
            Console.WriteLine("📊 COMPRESSION RATIOS");
            Console.WriteLine("================================");
            Console.WriteLine($"JSON → SQLite:      {((1.0 - (double)dbSize / jsonSize) * 100):F1}% reduction");
            Console.WriteLine($"JSON → Compressed:  {((1.0 - (double)compressedSize / jsonSize) * 100):F1}% reduction");
            Console.WriteLine($"SQLite → Compressed: {((1.0 - (double)compressedSize / dbSize) * 100):F1}% reduction");
        }
        finally
        {
            await connection.CloseAsync();
            await connection.DisposeAsync();
        }
    }

    private static string FormatFileSize(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len /= 1024;
        }
        return $"{len:0.##} {sizes[order]}";
    }

    private static int DetermineTestament(string bookName)
    {
        var oldTestamentBooks = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
            "Joshua", "Judges", "Ruth", "1 Samuel", "2 Samuel", "1 Kings", "2 Kings",
            "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther",
            "Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Solomon",
            "Isaiah", "Jeremiah", "Lamentations", "Ezekiel", "Daniel",
            "Hosea", "Joel", "Amos", "Obadiah", "Jonah", "Micah", "Nahum",
            "Habakkuk", "Zephaniah", "Haggai", "Zechariah", "Malachi"
        };

        return oldTestamentBooks.Contains(bookName) ? 1 : 2;
    }
}

// Data models matching your JSON structure
public class BibleBook
{
    public string BookName { get; set; } = "";
    public List<Chapter> lstChapters { get; set; } = new();
}

public class Chapter
{
    public int ChapterNumber { get; set; }
    public List<Verse> Verses { get; set; } = new();
}

public class Verse
{
    public int VerseNumber { get; set; }
    public string ChapterVerse { get; set; } = "";
}