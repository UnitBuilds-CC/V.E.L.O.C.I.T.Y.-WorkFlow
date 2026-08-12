#pragma warning disable RS1038 // This generator is in the same project as a code fix provider which requires Workspaces
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Velocity.Workflow.Generators;

[Generator]
public class DurableWorkflowGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var methodDeclarations = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (s, _) => s is MethodDeclarationSyntax m && m.AttributeLists.Count > 0,
                transform: static (ctx, _) => GetDurableMethod(ctx))
            .Where(static m => m != null);

        context.RegisterSourceOutput(methodDeclarations, static (spc, method) => Execute(spc, method!));
    }

    private static MethodDeclarationSyntax? GetDurableMethod(GeneratorSyntaxContext context)
    {
        var methodSyntax = (MethodDeclarationSyntax)context.Node;
        foreach (var attributeList in methodSyntax.AttributeLists)
        {
            foreach (var attribute in attributeList.Attributes)
            {
                if (attribute.Name.ToString().Contains("DurableWorkflow"))
                {
                    return methodSyntax;
                }
            }
        }
        return null;
    }

    private static void Execute(SourceProductionContext context, MethodDeclarationSyntax methodSyntax)
    {
        var classDeclaration = methodSyntax.Parent as ClassDeclarationSyntax;
        if (classDeclaration == null) return;

        var namespaceDeclaration = classDeclaration.Parent as BaseNamespaceDeclarationSyntax;
        string namespaceName = namespaceDeclaration?.Name.ToString() ?? "Velocity.Workflow.Generated";
        string className = classDeclaration.Identifier.ValueText;

        string source = StateMachineRewriter.GenerateRunnerClass(methodSyntax, namespaceName, className);
        context.AddSource($"{className}_{methodSyntax.Identifier.ValueText}_GeneratedRunner.g.cs", SourceText.From(source, Encoding.UTF8));
    }
}
