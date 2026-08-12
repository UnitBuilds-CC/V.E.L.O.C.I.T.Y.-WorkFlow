#pragma warning disable RS1038 // This analyzer is in the same project as a code fix provider which requires Workspaces
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Velocity.Workflow.Generators;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public class DeterminismAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticIdClock = "VEL0001";
    public const string DiagnosticIdGuid = "VEL0002";
    public const string DiagnosticIdRandom = "VEL0003";

    private static readonly DiagnosticDescriptor RuleClock = new(
        DiagnosticIdClock,
        "Non-deterministic system clock call inside Durable Workflow",
        "Method '{0}' calls '{1}' which is non-deterministic. Use WorkflowClock.UtcNow instead.",
        "Determinism",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor RuleGuid = new(
        DiagnosticIdGuid,
        "Non-deterministic Guid generation inside Durable Workflow",
        "Method '{0}' calls Guid.NewGuid() which is non-deterministic. Use WorkflowGuid.NewGuid() instead.",
        "Determinism",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor RuleRandom = new(
        DiagnosticIdRandom,
        "Non-deterministic Random instantiation inside Durable Workflow",
        "Method '{0}' instantiates System.Random which is non-deterministic.",
        "Determinism",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(RuleClock, RuleGuid, RuleRandom);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
        context.RegisterOperationAction(AnalyzePropertyAccess, OperationKind.PropertyReference);
        context.RegisterOperationAction(AnalyzeObjectCreation, OperationKind.ObjectCreation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        var invocation = (IInvocationOperation)context.Operation;
        var targetMethod = invocation.TargetMethod;

        if (targetMethod.ContainingType?.ToDisplayString() == "System.Guid" && targetMethod.Name == "NewGuid")
        {
            if (IsInsideDurableWorkflow(invocation))
            {
                var enclosing = context.ContainingSymbol?.Name ?? "WorkflowMethod";
                context.ReportDiagnostic(Diagnostic.Create(RuleGuid, invocation.Syntax.GetLocation(), enclosing));
            }
        }
    }

    private static void AnalyzePropertyAccess(OperationAnalysisContext context)
    {
        var propRef = (IPropertyReferenceOperation)context.Operation;
        var property = propRef.Property;

        if (property.ContainingType?.ToDisplayString() == "System.DateTime" && (property.Name == "UtcNow" || property.Name == "Now"))
        {
            if (IsInsideDurableWorkflow(propRef))
            {
                var enclosing = context.ContainingSymbol?.Name ?? "WorkflowMethod";
                context.ReportDiagnostic(Diagnostic.Create(RuleClock, propRef.Syntax.GetLocation(), enclosing, property.ToDisplayString()));
            }
        }
    }

    private static void AnalyzeObjectCreation(OperationAnalysisContext context)
    {
        var creation = (IObjectCreationOperation)context.Operation;
        if (creation.Type?.ToDisplayString() == "System.Random")
        {
            if (IsInsideDurableWorkflow(creation))
            {
                var enclosing = context.ContainingSymbol?.Name ?? "WorkflowMethod";
                context.ReportDiagnostic(Diagnostic.Create(RuleRandom, creation.Syntax.GetLocation(), enclosing, enclosing));
            }
        }
    }

    private static bool IsInsideDurableWorkflow(IOperation operation)
    {
        var methodSymbol = operation.SemanticModel?.GetEnclosingSymbol(operation.Syntax.SpanStart) as IMethodSymbol;
        if (methodSymbol == null) return false;

        foreach (var attr in methodSymbol.GetAttributes())
        {
            if (attr.AttributeClass?.Name is "DurableWorkflowAttribute" or "DurableWorkflow")
                return true;
        }

        if (methodSymbol.ContainingType != null)
        {
            foreach (var attr in methodSymbol.ContainingType.GetAttributes())
            {
                if (attr.AttributeClass?.Name is "DurableWorkflowAttribute" or "DurableWorkflow")
                    return true;
            }
        }

        return false;
    }
}
