using System.Diagnostics;
using System.Management;
using System.Reactive.Subjects;
using AutoMuteUsPortable.PocketBaseClient;
using AutoMuteUsPortable.Shared.Controller.Executor;
using AutoMuteUsPortable.Shared.Entity.ExecutorConfigurationBaseNS;
using AutoMuteUsPortable.Shared.Entity.ExecutorConfigurationNS;
using AutoMuteUsPortable.Shared.Entity.ProgressInfo;
using AutoMuteUsPortable.Shared.Utility;
using CliWrap;
using CliWrap.EventStream;
using FluentValidation;

namespace AutoMuteUsPortable.Executor;

public class ExecutorController : ExecutorControllerBase
{
    private readonly PocketBaseClientApplication _pocketBaseClientApplication = new();
    private readonly StreamWriter _outputStreamWriter;
    private readonly StreamWriter _errorStreamWriter;
    private CancellationTokenSource _forcefulCTS = new();
    private CancellationTokenSource _gracefulCTS = new();

    public ExecutorController(object executorConfiguration) : base(executorConfiguration)
    {
        #region Initialize stream writer

        _outputStreamWriter = new StreamWriter(OutputStream);
        _errorStreamWriter = new StreamWriter(ErrorStream);

        #endregion

        #region Check variables

        var binaryDirectory = Utils.PropertyByName<string>(executorConfiguration, "binaryDirectory");
        if (binaryDirectory == null)
            throw new InvalidDataException("binaryDirectory cannot be null");

        var binaryVersion = Utils.PropertyByName<string>(executorConfiguration, "binaryVersion");
        if (binaryVersion == null)
            throw new InvalidDataException("binaryVersion cannot be null");

        var version = Utils.PropertyByName<string>(executorConfiguration, "version");
        if (version == null) throw new InvalidDataException("version cannot be null");

        ExecutorType? type = Utils.PropertyByName<ExecutorType>(executorConfiguration, "type");
        if (type == null) throw new InvalidDataException("type cannot be null");

        var environmentVariables =
            Utils.PropertyByName<Dictionary<string, string>>(executorConfiguration, "environmentVariables");
        if (environmentVariables == null) throw new InvalidDataException("environmentVariables cannot be null");

        #endregion

        #region Create ExecutorConfiguration and validate

        ExecutorConfiguration tmp = new()
        {
            version = version,
            type = (ExecutorType)type,
            binaryVersion = binaryVersion,
            binaryDirectory = binaryDirectory,
            environmentVariables = environmentVariables
        };

        var validator = new ExecutorConfigurationValidator();
        validator.ValidateAndThrow(tmp);

        ExecutorConfiguration = tmp;

        #endregion
    }

    public ExecutorController(object computedSimpleSettings,
        object executorConfigurationBase) : base(computedSimpleSettings, executorConfigurationBase)
    {
        #region Initialize stream writer

        _outputStreamWriter = new StreamWriter(OutputStream);
        _errorStreamWriter = new StreamWriter(ErrorStream);

        #endregion

        #region Check variables

        var binaryDirectory = Utils.PropertyByName<string>(executorConfigurationBase, "binaryDirectory");
        if (binaryDirectory == null)
            throw new InvalidDataException("binaryDirectory cannot be null");

        var binaryVersion = Utils.PropertyByName<string>(executorConfigurationBase, "binaryVersion");
        if (binaryVersion == null)
            throw new InvalidDataException("binaryVersion cannot be null");

        var version = Utils.PropertyByName<string>(executorConfigurationBase, "version");
        if (version == null) throw new InvalidDataException("version cannot be null");

        ExecutorType? type = Utils.PropertyByName<ExecutorType>(executorConfigurationBase, "type");
        if (type == null) throw new InvalidDataException("type cannot be null");

        if (Utils.PropertyInfoByName(computedSimpleSettings, "port") == null)
            throw new InvalidDataException("port is not found in computedSimpleSettings");
        var port = Utils.PropertyByName<object>(computedSimpleSettings, "port");

        int? galactusPort = Utils.PropertyByName<int>(port!, "galactus");
        if (galactusPort == null) throw new InvalidDataException("galactusPort cannot be null");

        int? brokerPort = Utils.PropertyByName<int>(port!, "broker");
        if (brokerPort == null) throw new InvalidDataException("brokerPort cannot be null");

        int? redisPort = Utils.PropertyByName<int>(port!, "redis");
        if (redisPort == null) throw new InvalidDataException("redisPort cannot be null");

        int? postgresqlPort = Utils.PropertyByName<int>(port!, "postgresql");
        if (postgresqlPort == null) throw new InvalidDataException("postgresqlPort cannot be null");

        var postgresql = Utils.PropertyByName<object>(computedSimpleSettings, "postgresql");
        if (postgresql == null) throw new InvalidDataException("postgresql cannot be null");

        var postgresqlUsername = Utils.PropertyByName<string>(postgresql, "username");
        if (string.IsNullOrEmpty(postgresqlUsername))
            throw new InvalidDataException("postgresqlUsername cannot be null or empty");

        var postgresqlPassword = Utils.PropertyByName<string>(postgresql, "password");
        if (string.IsNullOrEmpty(postgresqlPassword))
            throw new InvalidDataException("postgresqlPassword cannot be null or empty");

        var discordToken = Utils.PropertyByName<string>(computedSimpleSettings, "discordToken");
        if (string.IsNullOrEmpty(discordToken)) throw new InvalidDataException("discordToken cannot be null or empty");

        #endregion

        #region Create ExecutorConfiguration and validate

        ExecutorConfiguration executorConfiguration = new()
        {
            version = version,
            type = (ExecutorType)type,
            binaryVersion = binaryVersion,
            binaryDirectory = binaryDirectory,
            environmentVariables = new Dictionary<string, string>
            {
                ["DISCORD_BOT_TOKEN"] = discordToken,
                ["HOST"] = $"http://localhost:{brokerPort}",
                ["POSTGRES_USER"] = postgresqlUsername,
                ["POSTGRES_PASS"] = postgresqlPassword,
                ["REDIS_ADDR"] = $"localhost:{redisPort}",
                ["GALACTUS_ADDR"] = $"http://localhost:{galactusPort}",
                ["POSTGRES_ADDR"] = $"localhost:{postgresqlPort}/automuteus",
                ["REDIS_PASS"] = "",
                ["DISABLE_LOG_FILE"] = "true"
            }
        };

        var validator = new ExecutorConfigurationValidator();
        validator.ValidateAndThrow(executorConfiguration);

        ExecutorConfiguration = executorConfiguration;

        #endregion
    }

    public override async Task Run(ISubject<ProgressInfo>? progress = null)
    {
        if (IsRunning) return;

        #region Setup progress

        var taskProgress = progress != null
            ? new TaskProgress(progress, new Dictionary<string, object?>
            {
                ["File integrity check"] = new List<string>
                {
                    "Checking file integrity",
                    "Downloading",
                    "Extracting"
                },
                ["Killing currently running server"] = null,
                ["Starting server"] = null
            })
            : null;

        #endregion

        #region Retrieve data from PocketBase

        var automuteus =
            _pocketBaseClientApplication.Data.AutomuteusCollection.FirstOrDefault(x =>
                x.Version == ExecutorConfiguration.binaryVersion);
        if (automuteus == null)
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not found in the database");
        if (automuteus.CompatibleExecutors.All(x => x.Version != ExecutorConfiguration.version))
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not compatible with Executor {ExecutorConfiguration.version}");

        #endregion

        #region Check file integrity

        var checksumUrl = Utils.GetChecksum(automuteus.Checksum);

        if (string.IsNullOrEmpty(checksumUrl))
        {
#if DEBUG
            // Continue without checksum file
            // TODO: log out as DEBUG Level
            taskProgress?.NextTask(3);
#else
                throw new InvalidDataException("Checksum cannot be null or empty");
#endif
        }
        else
        {
            using (var client = new HttpClient())
            {
                var res = await client.GetStringAsync(checksumUrl);
                var checksum = Utils.ParseChecksumText(res);
                var checksumProgress = taskProgress?.GetSubjectProgress();
                checksumProgress?.OnNext(new ProgressInfo
                {
                    name = string.Format("{0}のファイルの整合性を確認しています", ExecutorConfiguration.type),
                    IsIndeterminate = true
                });
                var invalidFiles = Utils.CompareChecksum(ExecutorConfiguration.binaryDirectory, checksum);
                taskProgress?.NextTask();

                if (0 < invalidFiles.Count)
                {
                    var downloadUrl = Utils.GetDownloadUrl(automuteus.DownloadUrl);
                    if (string.IsNullOrEmpty(downloadUrl))
                        throw new InvalidDataException("DownloadUrl cannot be null or empty");

                    var binaryPath = Path.Combine(ExecutorConfiguration.binaryDirectory,
                        Path.GetFileName(downloadUrl));

                    var downloadProgress = taskProgress?.GetProgress();
                    if (taskProgress?.ActiveLeafTask != null)
                        taskProgress.ActiveLeafTask.Name =
                            string.Format("{0}の実行に必要なファイルをダウンロードしています", ExecutorConfiguration.type);
                    await Utils.DownloadAsync(downloadUrl, binaryPath, downloadProgress);
                    taskProgress?.NextTask();

                    var extractProgress = taskProgress?.GetProgress();
                    if (taskProgress?.ActiveLeafTask != null)
                        taskProgress.ActiveLeafTask.Name =
                            string.Format("{0}の実行に必要なファイルを解凍しています", ExecutorConfiguration.type);
                    Utils.ExtractZip(binaryPath, extractProgress);
                    taskProgress?.NextTask();
                }
                else
                {
                    taskProgress?.NextTask(2);
                }
            }
        }

        #endregion

        #region Search for currently running process and kill it

        var fileName = Path.Combine(ExecutorConfiguration.binaryDirectory, "automuteus.exe");

        var killingProgress = taskProgress?.GetSubjectProgress();
        killingProgress?.OnNext(new ProgressInfo
        {
            name = string.Format("既に起動している{0}を終了しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });
        var wmiQueryString =
            $"SELECT ProcessId FROM Win32_Process WHERE ExecutablePath = '{fileName.Replace(@"\", @"\\")}'";
        using (var searcher = new ManagementObjectSearcher(wmiQueryString))
        using (var results = searcher.Get())
        {
            foreach (var result in results)
                try
                {
                    var processId = (uint)result["ProcessId"];
                    var process = Process.GetProcessById((int)processId);

                    process.Kill();
                }
                catch
                {
                    // ignored
                }
        }

        taskProgress?.NextTask();

        #endregion

        #region Start server

        var startProgress = taskProgress?.GetSubjectProgress();
        startProgress?.OnNext(new ProgressInfo
        {
            name = string.Format("{0}を起動しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });
        var cmd = Cli.Wrap(Path.Combine(ExecutorConfiguration.binaryDirectory, @"automuteus.exe"))
            .WithEnvironmentVariables(ExecutorConfiguration.environmentVariables!)
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError));

        _forcefulCTS = new CancellationTokenSource();
        _gracefulCTS = new CancellationTokenSource();
        try
        {
            cmd.Observe(Console.OutputEncoding, Console.OutputEncoding, _forcefulCTS.Token, _gracefulCTS.Token)
                .Subscribe(
                    e =>
                    {
                        switch (e)
                        {
                            case StartedCommandEvent started:
                                OnStart();
                                break;
                            case ExitedCommandEvent exited:
                                OnStop();
                                break;
                        }
                    });
        }
        catch (OperationCanceledException ex)
        {
            // ignored
        }
        catch
        {
            // ignored
            // TODO: handle exception more elegantly
        }

        taskProgress?.NextTask();

        #endregion
    }

    public override Task Stop(ISubject<ProgressInfo>? progress = null)
    {
        if (!IsRunning) return Task.CompletedTask;

        #region Stop server

        progress?.OnNext(new ProgressInfo
        {
            name = string.Format("{0}を終了しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });
        _gracefulCTS.Cancel();
        return Task.CompletedTask;

        #endregion
    }

    public override async Task Restart(ISubject<ProgressInfo>? progress = null)
    {
        if (!IsRunning) return;

        #region Setup progress

        var taskProgress = progress != null
            ? new TaskProgress(progress, new List<string>
            {
                "Stopping",
                "Starting"
            })
            : null;

        #endregion

        #region Stop server

        var stopProgress = taskProgress?.GetSubjectProgress();
        await Stop(stopProgress);
        taskProgress?.NextTask();

        #endregion

        #region Start server

        var runProgress = taskProgress?.GetSubjectProgress();
        await Run(runProgress);
        taskProgress?.NextTask();

        #endregion
    }

    public override async Task Install(
        Dictionary<ExecutorType, ExecutorControllerBase> executors, ISubject<ProgressInfo>? progress = null)
    {
        #region Setup progress

        var taskProgress = progress != null
            ? new TaskProgress(progress, new Dictionary<string, object?>
            {
                ["Downloading"] = null,
                ["Extracting"] = null,
                ["Starting"] = null,
                ["Database initialization"] = new List<string>
                {
                    "Creating new database",
                    "Initializing"
                },
                ["Stopping"] = null
            })
            : null;

        #endregion

        #region Retrieve data from PocketBase

        var automuteus =
            _pocketBaseClientApplication.Data.AutomuteusCollection.FirstOrDefault(x =>
                x.Version == ExecutorConfiguration.binaryVersion);
        if (automuteus == null)
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not found in the database");
        if (automuteus.CompatibleExecutors.All(x => x.Version != ExecutorConfiguration.version))
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not compatible with Executor {ExecutorConfiguration.version}");
        string? downloadUrl = null;
        if (automuteus.DownloadUrl != null) downloadUrl = Utils.GetDownloadUrl(automuteus.DownloadUrl);
        if (string.IsNullOrEmpty(downloadUrl))
            throw new InvalidDataException("DownloadUrl cannot be null or empty");

        #endregion

        #region Download

        if (!Directory.Exists(ExecutorConfiguration.binaryDirectory))
            Directory.CreateDirectory(ExecutorConfiguration.binaryDirectory);

        var binaryPath = Path.Combine(ExecutorConfiguration.binaryDirectory,
            Path.GetFileName(downloadUrl));

        var downloadProgress = taskProgress?.GetProgress();
        if (taskProgress?.ActiveLeafTask != null)
            taskProgress.ActiveLeafTask.Name = string.Format("{0}の実行に必要なファイルをダウンロードしています", ExecutorConfiguration.type);
        await Utils.DownloadAsync(downloadUrl, binaryPath, downloadProgress);
        taskProgress?.NextTask();

        #endregion

        #region Extract

        var extractProgress = taskProgress?.GetProgress();
        if (taskProgress?.ActiveLeafTask != null)
            taskProgress.ActiveLeafTask.Name = string.Format("{0}の実行に必要なファイルを解凍しています", ExecutorConfiguration.type);
        Utils.ExtractZip(binaryPath, extractProgress);
        taskProgress?.NextTask();

        #endregion

        #region Start PostgreSQL to initialize database

        if (!executors.ContainsKey(ExecutorType.postgresql))
            throw new InvalidDataException("PostgreSQL executor is not found");
        var postgresqlExecutor = executors[ExecutorType.postgresql];
        var startProgress = taskProgress?.GetSubjectProgress();
        await postgresqlExecutor.Run(startProgress);
        taskProgress?.NextTask();

        #endregion

        #region Create database for automuteus

        var postgresqlAddress = ExecutorConfiguration.environmentVariables["POSTGRES_ADDR"];
        var postgresqlUsername = ExecutorConfiguration.environmentVariables["POSTGRES_USER"];
        var postgresqlPassword = ExecutorConfiguration.environmentVariables["POSTGRES_PASS"];

        if (!postgresqlExecutor.ExecutorConfiguration.environmentVariables.ContainsKey("POSTGRESQL_PORT"))
            throw new InvalidDataException(
                "POSTGRESQL_PORT is not found in environment variables of PostgreSQL executor");
        var postgresqlPort = postgresqlExecutor.ExecutorConfiguration.environmentVariables["POSTGRESQL_PORT"];

        var createDatabaseProgress = taskProgress?.GetSubjectProgress();
        createDatabaseProgress?.OnNext(new ProgressInfo
        {
            name = "データベースを作成しています",
            IsIndeterminate = true
        });
        await Cli.Wrap(Path.Combine(postgresqlExecutor.ExecutorConfiguration.binaryDirectory, @"bin\psql.exe"))
            .WithArguments(
                $@"--no-password --command=""CREATE DATABASE automuteus"" postgresql://{postgresqlUsername}:{postgresqlPassword}@localhost:{postgresqlPort}/postgres")
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError))
            .ExecuteAsync();

        taskProgress?.NextTask();

        #endregion

        #region Initialize database

        var queryFile = Path.GetTempFileName();
        var query = @"
create table guilds
(
    guild_id numeric PRIMARY KEY,
    guild_name VARCHAR(100) NOT NULL,
    premium smallint NOT NULL,
    tx_time_unix integer,
    transferred_to numeric references guilds(guild_id),
    inherits_from numeric references guilds(guild_id)
);

create table games
(
    game_id      bigserial PRIMARY KEY,
    guild_id     numeric references guilds ON DELETE CASCADE, --if the guild is deleted, delete their games, too
    connect_code CHAR(8) NOT NULL,
    start_time   integer NOT NULL,                            --2038 problem, but I do not care
    win_type     smallint,                                    --imposter win, crewmate win, etc
    end_time     integer                                      --2038 problem, but I do not care
);

-- links userIDs to their hashed variants. Allows for deletion of users without deleting underlying game_event data
create table users
(
    user_id numeric PRIMARY KEY,
    opt     boolean, --opt-out to data collection
    vote_time_unix integer --if they've ever voted for the bot on top.gg
);

create table game_events
(
    event_id   bigserial,
    user_id    numeric,                                              --actually references users, but can be null, so implied reference, not literal
    game_id    bigint   NOT NULL references games ON DELETE CASCADE, --delete all events from a game that's deleted
    event_time integer  NOT NULL,                                    --2038 problem, but I do not care
    event_type smallint NOT NULL,
    payload    jsonb
);

create table users_games
(
    user_id numeric REFERENCES users ON DELETE CASCADE, --if a user gets deleted, delete their linked games
    guild_id numeric REFERENCES guilds ON DELETE CASCADE, --if a guild is deleted, delete all linked games
    game_id bigint REFERENCES games ON DELETE CASCADE, --if a game is deleted, delete all linked users_games
    player_name VARCHAR(10) NOT NULL,
    player_color smallint NOT NULL,
    player_role smallint NOT NULL,
    player_won bool NOT NULL,
    PRIMARY KEY (user_id, game_id)
);

create index guilds_id_index ON guilds (guild_id); --query guilds by ID
create index guilds_premium_index ON guilds (premium); --query guilds by prem status

create index games_game_id_index ON games (game_id); --query games by ID
create index games_guild_id_index ON games (guild_id); --query games by guild ID
create index games_win_type_index on games (win_type); --query games by win type
create index games_connect_code_index on games (connect_code); --query games by connect code

create index users_user_id_index ON users (user_id); --query for user info by their ID

create index users_games_user_id_index ON users_games (user_id); --query games by user ID
create index users_games_game_id_index ON users_games (game_id); --query games by game ID
create index users_games_guild_id_index ON users_games (guild_id); --query games by guild ID
create index users_games_role_index ON users_games (player_role); --query games by win status
create index users_games_won_index ON users_games (player_won); --query games by win status

create index game_events_game_id_index on game_events (game_id); --query for game events by the game ID
create index game_events_user_id_index on game_events (user_id); --query for game events by the user ID
";
        await File.WriteAllTextAsync(queryFile, query);

        var initializeProgress = taskProgress?.GetSubjectProgress();
        initializeProgress?.OnNext(new ProgressInfo
        {
            name = "データベースを初期化しています",
            IsIndeterminate = true
        });

        await Cli.Wrap(Path.Combine(postgresqlExecutor.ExecutorConfiguration.binaryDirectory, @"bin\psql.exe"))
            .WithArguments(
                $@"--no-password --file=""{queryFile}"" postgresql://{postgresqlUsername}:{postgresqlPassword}@{postgresqlAddress}")
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError))
            .ExecuteAsync();

        taskProgress?.NextTask();

        #endregion

        #region Stop PostgreSQL

        var stopProgress = taskProgress?.GetSubjectProgress();
        await postgresqlExecutor.Stop(stopProgress);
        taskProgress?.NextTask();

        #endregion
    }

    public override Task Update(
        Dictionary<ExecutorType, ExecutorControllerBase> executors, object oldExecutorConfiguration,
        ISubject<ProgressInfo>? progress = null)
    {
        return Task.CompletedTask;
    }

    private void ProcessStandardOutput(string text)
    {
        _outputStreamWriter.Write(text);
    }

    private void ProcessStandardError(string text)
    {
        _errorStreamWriter.Write(text);
    }
}