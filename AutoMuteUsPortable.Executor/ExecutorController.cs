using System.Reactive.Subjects;
using AutoMuteUsPortable.Shared.Controller.Executor;
using AutoMuteUsPortable.Shared.Entity.ExecutorConfigurationBaseNS;
using AutoMuteUsPortable.Shared.Entity.ProgressInfo;

namespace AutoMuteUsPortable.Executor;

public class ExecutorController : ExecutorControllerBase
{
    public new static Dictionary<string, Parameter> InstallParameters = new();
    public new static Dictionary<string, Parameter> UpdateParameters = new();

    public ExecutorController(object executorConfiguration) : base(executorConfiguration)
    {
        throw new NotImplementedException();
    }

    public ExecutorController(object computedSimpleSettings,
        object executorConfigurationBase) : base(computedSimpleSettings, executorConfigurationBase)
    {
        throw new NotImplementedException();
    }

    public override async Task Run(ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public override async Task Stop(ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public override async Task Restart(ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public override async Task Install(Dictionary<string, string> parameters,
        Dictionary<ExecutorType, ExecutorControllerBase> executors, ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public override async Task Update(Dictionary<string, string> parameters,
        Dictionary<ExecutorType, ExecutorControllerBase> executors, ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public override async Task InstallBySimpleSettings(object simpleSettings, object executorConfigurationBase,
        Dictionary<ExecutorType, ExecutorControllerBase> executors,
        ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public override async Task UpdateBySimpleSettings(object simpleSettings, object executorConfigurationBase,
        Dictionary<ExecutorType, ExecutorControllerBase> executors,
        ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }
}