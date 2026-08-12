using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace temporal2velocity;

/// <summary>
/// AST-based transpiler engine for converting Temporal C# SDK code to VELOCITY-WorkFlow.
/// Uses Roslyn syntax tree parsing and rewriting instead of regex patterns.
///
/// This replaces the regex-based TranspilerEngine for C# inputs, providing:
///   - Proper syntax tree walking (no false matches in strings/comments)
///   - Semantic-aware rewrites (type-qualified member access)
///   - Attribute injection via syntax tree manipulation
///   - Determinism rewrites with full expression context
///   - Signal/query handler detection via attribute inspection
///
/// TypeScript inputs still use the regex-based TranspilerEngine (no Roslyn for TS).
/// </summary>
public static class AstTranspilerEngine
{
    /// <summary>
    /// Statistics from an AST-based transpilation run.
    /// </summary>
    public class AstTranspileStats
    {
        public int UsingDirectivesRewritten;
        public int MemberAccesses_Rewritten;
        public int ObjectCreations_Rewritten;
        public int Attributes_Injected;
        public int SignalHandlers_Converted;
        public int QueryHandlers_Converted;
        public int ChildWorkflows_Converted;
        public int VersionGuards_Removed;
        public int TimerCalls_Converted;
        public int TotalNodes_Visited;
        public int TotalReplacements;
        public string SourceMode = "C#-AST";
    }

    /// <summary>
    /// Transpile C# source code using Roslyn AST rewriting.
    /// </summary>
    public static string Transpile(string sourceCode, out AstTranspileStats stats)
    {
        stats = new AstTranspileStats();
        if (string.IsNullOrWhiteSpace(sourceCode)) return sourceCode;

        // Parse into syntax tree
        var tree = CSharpSyntaxTree.ParseText(sourceCode);
        var root = tree.GetRoot();

        // Phase 1: Rewrite using directives
        root = RewriteUsingDirectives(root, stats);

        // Phase 2: Rewrite member accesses (DateTime.UtcNow → WorkflowClock.UtcNow, etc.)
        root = RewriteMemberAccesses(root, stats);

        // Phase 3: Rewrite object creations (new Random() → new WorkflowRandom())
        root = RewriteObjectCreations(root, stats);

        // Phase 4: Inject [DurableWorkflow] attributes on workflow classes
        root = InjectDurableAttributes(root, stats);

        // Phase 5: Convert signal/query handlers
        root = ConvertSignalQueryHandlers(root, stats);

        // Phase 6: Convert child workflow invocations
        root = ConvertChildWorkflows(root, stats);

        // Phase 7: Remove version guards
        root = RemoveVersionGuards(root, stats);

        // Phase 8: Convert timer calls
        root = ConvertTimerCalls(root, stats);

        stats.TotalReplacements = stats.UsingDirectivesRewritten + stats.MemberAccesses_Rewritten +
            stats.ObjectCreations_Rewritten + stats.Attributes_Injected +
            stats.SignalHandlers_Converted + stats.QueryHandlers_Converted +
            stats.ChildWorkflows_Converted + stats.VersionGuards_Removed +
            stats.TimerCalls_Converted;

        return root.ToFullString();
    }

    /// <summary>
    /// Transpile without stats output.
    /// </summary>
    public static string Transpile(string sourceCode)
    {
        return Transpile(sourceCode, out _);
    }

    // ─── Phase 1: Using Directive Rewriting ──────────────────────────────────

    private static SyntaxNode RewriteUsingDirectives(SyntaxNode root, AstTranspileStats stats)
    {
        var usingDirectives = root.DescendantNodes().OfType<UsingDirectiveSyntax>().ToList();

        foreach (var usingDir in usingDirectives)
        {
            string name = usingDir.Name?.ToString() ?? "";
            string? replacement = name switch
            {
                "Temporalio.Client" => "Velocity.Workflow.Core",
                "Temporalio.Workflows" => "Velocity.Workflow.Core",
                "Temporalio.Activities" => "Velocity.Workflow.Core",
                "Temporalio.Exceptions" => "Velocity.Workflow.Core",
                "Temporalio.Converters" => null, // Remove — Velocity uses slab serialization
                _ => null
            };

            if (replacement != null)
            {
                var newUsing = usingDir.WithName(
                    SyntaxFactory.ParseName(replacement)
                        .WithLeadingTrivia(usingDir.Name?.GetLeadingTrivia() ?? default)
                        .WithTrailingTrivia(usingDir.Name?.GetTrailingTrivia() ?? default));
                root = root.ReplaceNode(usingDir, newUsing);
                stats.UsingDirectivesRewritten++;
            }
            else if (name == "Temporalio.Converters")
            {
                // Replace with a comment by removing the using and adding a comment trivia
                var comment = SyntaxFactory.Comment("// Velocity uses built-in slab serialization");
                var newUsing = usingDir
                    .WithName(SyntaxFactory.ParseName("_removed_"))
                    .WithLeadingTrivia(comment);
                root = root.ReplaceNode(usingDir, newUsing);
                stats.UsingDirectivesRewritten++;
            }
        }

        return root;
    }

    // ─── Phase 2: Member Access Rewriting ────────────────────────────────────

    private static SyntaxNode RewriteMemberAccesses(SyntaxNode root, AstTranspileStats stats)
    {
        var memberAccesses = root.DescendantNodes()
            .OfType<MemberAccessExpressionSyntax>()
            .ToList();

        var replacements = new Dictionary<SyntaxNode, ExpressionSyntax>();

        foreach (var ma in memberAccesses)
        {
            stats.TotalNodes_Visited++;
            string expression = ma.Expression.ToString();
            string memberName = ma.Name.Identifier.ValueText;

            // DateTime.UtcNow → WorkflowClock.UtcNow
            if (expression == "DateTime" && (memberName == "UtcNow" || memberName == "Now"))
            {
                replacements[ma] = SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("WorkflowClock"),
                    SyntaxFactory.IdentifierName("UtcNow"))
                    .WithLeadingTrivia(ma.GetLeadingTrivia())
                    .WithTrailingTrivia(ma.GetTrailingTrivia());
                stats.MemberAccesses_Rewritten++;
            }
            // Guid.NewGuid → WorkflowGuid.NewGuid
            else if (expression == "Guid" && memberName == "NewGuid")
            {
                replacements[ma] = SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("WorkflowGuid"),
                    SyntaxFactory.IdentifierName("NewGuid"))
                    .WithLeadingTrivia(ma.GetLeadingTrivia())
                    .WithTrailingTrivia(ma.GetTrailingTrivia());
                stats.MemberAccesses_Rewritten++;
            }
            // Workflow.Timer.Sleep → Task.Delay
            else if (expression == "Workflow.Timer" && memberName == "Sleep")
            {
                replacements[ma] = SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("Task"),
                    SyntaxFactory.IdentifierName("Delay"))
                    .WithLeadingTrivia(ma.GetLeadingTrivia())
                    .WithTrailingTrivia(ma.GetTrailingTrivia());
                stats.TimerCalls_Converted++;
            }
        }

        return root.ReplaceNodes(replacements.Keys, (orig, _) => replacements[orig]);
    }

    // ─── Phase 3: Object Creation Rewriting ──────────────────────────────────

    private static SyntaxNode RewriteObjectCreations(SyntaxNode root, AstTranspileStats stats)
    {
        var creations = root.DescendantNodes()
            .OfType<ObjectCreationExpressionSyntax>()
            .ToList();

        var replacements = new Dictionary<SyntaxNode, ExpressionSyntax>();

        foreach (var creation in creations)
        {
            stats.TotalNodes_Visited++;
            string typeName = creation.Type.ToString();

            // new Random() → new WorkflowRandom()
            if (typeName == "Random" || typeName == "System.Random")
            {
                var newCreation = SyntaxFactory.ObjectCreationExpression(
                    SyntaxFactory.ParseTypeName("WorkflowRandom"))
                    .WithArgumentList(creation.ArgumentList)
                    .WithLeadingTrivia(creation.GetLeadingTrivia())
                    .WithTrailingTrivia(creation.GetTrailingTrivia());
                replacements[creation] = newCreation;
                stats.ObjectCreations_Rewritten++;
            }
        }

        return root.ReplaceNodes(replacements.Keys, (orig, _) => replacements[orig]);
    }

    // ─── Phase 4: Durable Attribute Injection ────────────────────────────────

    private static SyntaxNode InjectDurableAttributes(SyntaxNode root, AstTranspileStats stats)
    {
        var classDeclarations = root.DescendantNodes()
            .OfType<ClassDeclarationSyntax>()
            .ToList();

        foreach (var classDecl in classDeclarations)
        {
            stats.TotalNodes_Visited++;

            // Check if class already has [DurableWorkflow] or inherits WorkflowBase
            bool hasDurableAttr = classDecl.AttributeLists
                .SelectMany(al => al.Attributes)
                .Any(a => a.Name.ToString().Contains("DurableWorkflow"));

            if (hasDurableAttr) continue;

            // Check if the class contains async methods (likely a workflow)
            bool hasAsyncMethods = classDecl.DescendantNodes()
                .OfType<MethodDeclarationSyntax>()
                .Any(m => m.Modifiers.Any(SyntaxKind.AsyncKeyword) ||
                          m.DescendantNodes().OfType<AwaitExpressionSyntax>().Any());

            if (!hasAsyncMethods) continue;

            // Inject [DurableWorkflow] attribute
            var attribute = SyntaxFactory.Attribute(SyntaxFactory.ParseName("DurableWorkflow"));
            var attributeList = SyntaxFactory.AttributeList(
                SyntaxFactory.SingletonSeparatedList(attribute));

            var newClass = classDecl.AddAttributeLists(attributeList);
            root = root.ReplaceNode(classDecl, newClass);
            stats.Attributes_Injected++;
        }

        return root;
    }

    // ─── Phase 5: Signal/Query Handler Conversion ────────────────────────────

    private static SyntaxNode ConvertSignalQueryHandlers(SyntaxNode root, AstTranspileStats stats)
    {
        var methods = root.DescendantNodes()
            .OfType<MethodDeclarationSyntax>()
            .ToList();

        foreach (var method in methods)
        {
            stats.TotalNodes_Visited++;
            var attrs = method.AttributeLists.SelectMany(al => al.Attributes).ToList();

            foreach (var attr in attrs)
            {
                string attrName = attr.Name.ToString();

                // [WorkflowSignal] → [VelocitySignal("methodName")]
                if (attrName == "WorkflowSignal" || attrName == "Signal")
                {
                    string methodName = method.Identifier.ValueText;
                    var newAttr = SyntaxFactory.Attribute(
                        SyntaxFactory.ParseName("VelocitySignal"),
                        SyntaxFactory.AttributeArgumentList(
                            SyntaxFactory.SingletonSeparatedList(
                                SyntaxFactory.AttributeArgument(
                                    SyntaxFactory.LiteralExpression(
                                        SyntaxKind.StringLiteralExpression,
                                        SyntaxFactory.Literal(methodName))))));

                    var newAttrList = SyntaxFactory.AttributeList(
                        SyntaxFactory.SingletonSeparatedList(newAttr))
                        .WithLeadingTrivia(attr.Parent!.GetLeadingTrivia())
                        .WithTrailingTrivia(attr.Parent!.GetTrailingTrivia());

                    root = root.ReplaceNode(attr.Parent!, newAttrList);
                    stats.SignalHandlers_Converted++;
                }

                // [WorkflowQuery] → [VelocityQuery("methodName")]
                if (attrName == "WorkflowQuery" || attrName == "Query")
                {
                    string methodName = method.Identifier.ValueText;
                    var newAttr = SyntaxFactory.Attribute(
                        SyntaxFactory.ParseName("VelocityQuery"),
                        SyntaxFactory.AttributeArgumentList(
                            SyntaxFactory.SingletonSeparatedList(
                                SyntaxFactory.AttributeArgument(
                                    SyntaxFactory.LiteralExpression(
                                        SyntaxKind.StringLiteralExpression,
                                        SyntaxFactory.Literal(methodName))))));

                    var newAttrList = SyntaxFactory.AttributeList(
                        SyntaxFactory.SingletonSeparatedList(newAttr))
                        .WithLeadingTrivia(attr.Parent!.GetLeadingTrivia())
                        .WithTrailingTrivia(attr.Parent!.GetTrailingTrivia());

                    root = root.ReplaceNode(attr.Parent!, newAttrList);
                    stats.QueryHandlers_Converted++;
                }
            }
        }

        return root;
    }

    // ─── Phase 6: Child Workflow Conversion ──────────────────────────────────

    private static SyntaxNode ConvertChildWorkflows(SyntaxNode root, AstTranspileStats stats)
    {
        var invocations = root.DescendantNodes()
            .OfType<InvocationExpressionSyntax>()
            .ToList();

        var replacements = new Dictionary<SyntaxNode, ExpressionSyntax>();

        foreach (var invocation in invocations)
        {
            stats.TotalNodes_Visited++;
            string expr = invocation.Expression.ToString();

            // Workflow.ExecuteChildAsync<T>(...) → ctx.ExecuteChildWorkflowAsync(...)
            if (expr.Contains("Workflow.ExecuteChildAsync") || expr.Contains("Workflow.ExecuteChild"))
            {
                var newExpr = SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("ctx"),
                    SyntaxFactory.IdentifierName("ExecuteChildWorkflowAsync"))
                    .WithLeadingTrivia(invocation.Expression.GetLeadingTrivia());

                replacements[invocation.Expression] = newExpr;
                stats.ChildWorkflows_Converted++;
            }
        }

        if (replacements.Count > 0)
        {
            root = root.ReplaceNodes(replacements.Keys, (orig, _) => replacements[orig]);
        }

        return root;
    }

    // ─── Phase 7: Version Guard Removal ──────────────────────────────────────

    private static SyntaxNode RemoveVersionGuards(SyntaxNode root, AstTranspileStats stats)
    {
        var invocations = root.DescendantNodes()
            .OfType<InvocationExpressionSyntax>()
            .ToList();

        foreach (var invocation in invocations)
        {
            stats.TotalNodes_Visited++;
            string expr = invocation.Expression.ToString();

            if (expr.Contains("Workflow.GetVersion") || expr.Contains("Workflow.GetVersionAsync"))
            {
                // Replace the containing statement with a comment
                var statement = invocation.Ancestors().OfType<ExpressionStatementSyntax>().FirstOrDefault();
                if (statement != null)
                {
                    var comment = SyntaxFactory.Comment(
                        "// Stripped legacy version guard — Velocity uses slab schema evolution");
                    var trailingTrivia = statement.GetTrailingTrivia().Add(
                        SyntaxFactory.EndOfLine(Environment.NewLine));

                    root = root.ReplaceNode(statement,
                        SyntaxFactory.EmptyStatement()
                            .WithLeadingTrivia(comment)
                            .WithTrailingTrivia(trailingTrivia));
                    stats.VersionGuards_Removed++;
                }
            }
        }

        return root;
    }

    // ─── Phase 8: Timer Call Conversion ──────────────────────────────────────

    private static SyntaxNode ConvertTimerCalls(SyntaxNode root, AstTranspileStats stats)
    {
        var invocations = root.DescendantNodes()
            .OfType<InvocationExpressionSyntax>()
            .ToList();

        var replacements = new Dictionary<SyntaxNode, ExpressionSyntax>();

        foreach (var invocation in invocations)
        {
            string expr = invocation.Expression.ToString();

            // Workflow.Delay(...) → Task.Delay(...)
            if (expr == "Workflow.Delay")
            {
                replacements[invocation.Expression] = SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("Task"),
                    SyntaxFactory.IdentifierName("Delay"))
                    .WithLeadingTrivia(invocation.Expression.GetLeadingTrivia());
                stats.TimerCalls_Converted++;
            }
        }

        if (replacements.Count > 0)
        {
            root = root.ReplaceNodes(replacements.Keys, (orig, _) => replacements[orig]);
        }

        return root;
    }
}
