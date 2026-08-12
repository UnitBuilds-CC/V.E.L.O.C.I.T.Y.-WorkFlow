using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Velocity.Workflow.Generators;

/// <summary>
/// CodeFix provider that auto-rewrites non-deterministic API calls inside
/// [DurableWorkflow]-annotated methods into their deterministic equivalents:
///   DateTime.UtcNow  → WorkflowClock.UtcNow
///   DateTime.Now     → WorkflowClock.UtcNow
///   Guid.NewGuid()   → WorkflowGuid.NewGuid()
///   new System.Random() → new WorkflowRandom()
///
/// This implements the base.md vision: "the compiler should silently replace
/// DateTime.UtcNow with WorkflowClock.UtcNow" — compile-time determinism rewriting.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(DeterminismCodeFixProvider)), Shared]
public class DeterminismCodeFixProvider : CodeFixProvider
{
    public override ImmutableArray<string> FixableDiagnosticIds =>
        ImmutableArray.Create(
            DeterminismAnalyzer.DiagnosticIdClock,
            DeterminismAnalyzer.DiagnosticIdGuid,
            DeterminismAnalyzer.DiagnosticIdRandom);

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);
        if (root == null) return;

        foreach (var diagnostic in context.Diagnostics)
        {
            var diagnosticSpan = diagnostic.Location.SourceSpan;
            var node = root.FindNode(diagnosticSpan);

            switch (diagnostic.Id)
            {
                case DeterminismAnalyzer.DiagnosticIdClock:
                    // DateTime.UtcNow → WorkflowClock.UtcNow
                    if (node is IdentifierNameSyntax identifier &&
                        (identifier.Identifier.ValueText == "UtcNow" || identifier.Identifier.ValueText == "Now"))
                    {
                        context.RegisterCodeFix(
                            CodeAction.Create(
                                title: "Replace with WorkflowClock.UtcNow",
                                createChangedDocument: c => ReplaceWithWorkflowClock(context.Document, node, c),
                                equivalenceKey: "UseWorkflowClock"),
                            diagnostic);
                    }
                    else if (node is MemberAccessExpressionSyntax memberAccess &&
                             memberAccess.Name.Identifier.ValueText is "UtcNow" or "Now")
                    {
                        context.RegisterCodeFix(
                            CodeAction.Create(
                                title: "Replace with WorkflowClock.UtcNow",
                                createChangedDocument: c => ReplaceMemberAccessWithWorkflowClock(context.Document, memberAccess, c),
                                equivalenceKey: "UseWorkflowClock"),
                            diagnostic);
                    }
                    break;

                case DeterminismAnalyzer.DiagnosticIdGuid:
                    // Guid.NewGuid() → WorkflowGuid.NewGuid()
                    if (node is IdentifierNameSyntax guidId && guidId.Identifier.ValueText == "NewGuid")
                    {
                        context.RegisterCodeFix(
                            CodeAction.Create(
                                title: "Replace with WorkflowGuid.NewGuid()",
                                createChangedDocument: c => ReplaceWithWorkflowGuid(context.Document, node, c),
                                equivalenceKey: "UseWorkflowGuid"),
                            diagnostic);
                    }
                    break;

                case DeterminismAnalyzer.DiagnosticIdRandom:
                    // new System.Random() → new WorkflowRandom()
                    if (node is IdentifierNameSyntax randomId && randomId.Identifier.ValueText == "Random")
                    {
                        context.RegisterCodeFix(
                            CodeAction.Create(
                                title: "Replace with WorkflowRandom",
                                createChangedDocument: c => ReplaceWithWorkflowRandom(context.Document, node, c),
                                equivalenceKey: "UseWorkflowRandom"),
                            diagnostic);
                    }
                    break;
            }
        }
    }

    /// <summary>
    /// Replace DateTime.UtcNow / DateTime.Now with WorkflowClock.UtcNow.
    /// Handles the case where the node is just the property name identifier.
    /// </summary>
    private static async Task<Document> ReplaceWithWorkflowClock(
        Document document, SyntaxNode node, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);
        if (root == null) return document;

        // The diagnostic is on the property name (UtcNow/Now).
        // We need to replace the entire member access (DateTime.UtcNow) with WorkflowClock.UtcNow.
        var parent = node.Parent;
        if (parent is MemberAccessExpressionSyntax memberAccess)
        {
            var newExpression = SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("WorkflowClock"),
                SyntaxFactory.IdentifierName("UtcNow"))
                .WithLeadingTrivia(memberAccess.GetLeadingTrivia())
                .WithTrailingTrivia(memberAccess.GetTrailingTrivia());

            var newRoot = root.ReplaceNode(memberAccess, newExpression);
            return document.WithSyntaxRoot(newRoot);
        }

        // Fallback: just replace the identifier
        var replacement = SyntaxFactory.IdentifierName("WorkflowClock.UtcNow")
            .WithLeadingTrivia(node.GetLeadingTrivia())
            .WithTrailingTrivia(node.GetTrailingTrivia());
        return document.WithSyntaxRoot(root.ReplaceNode(node, replacement));
    }

    /// <summary>
    /// Replace a full member access expression (DateTime.UtcNow) with WorkflowClock.UtcNow.
    /// </summary>
    private static async Task<Document> ReplaceMemberAccessWithWorkflowClock(
        Document document, MemberAccessExpressionSyntax memberAccess, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);
        if (root == null) return document;

        var newExpression = SyntaxFactory.MemberAccessExpression(
            SyntaxKind.SimpleMemberAccessExpression,
            SyntaxFactory.IdentifierName("WorkflowClock"),
            SyntaxFactory.IdentifierName("UtcNow"))
            .WithLeadingTrivia(memberAccess.GetLeadingTrivia())
            .WithTrailingTrivia(memberAccess.GetTrailingTrivia());

        var newRoot = root.ReplaceNode(memberAccess, newExpression);
        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    /// Replace Guid.NewGuid() with WorkflowGuid.NewGuid().
    /// The node is the method name identifier; we need to replace the containing member access.
    /// </summary>
    private static async Task<Document> ReplaceWithWorkflowGuid(
        Document document, SyntaxNode node, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);
        if (root == null) return document;

        // The diagnostic is on "NewGuid" identifier. Replace the containing member access.
        var parent = node.Parent;
        if (parent is MemberAccessExpressionSyntax memberAccess)
        {
            var newMemberAccess = SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("WorkflowGuid"),
                SyntaxFactory.IdentifierName("NewGuid"))
                .WithLeadingTrivia(memberAccess.GetLeadingTrivia())
                .WithTrailingTrivia(memberAccess.GetTrailingTrivia());

            var newRoot = root.ReplaceNode(memberAccess, newMemberAccess);
            return document.WithSyntaxRoot(newRoot);
        }

        // Fallback: replace identifier directly
        var replacement = SyntaxFactory.IdentifierName("WorkflowGuid.NewGuid")
            .WithLeadingTrivia(node.GetLeadingTrivia())
            .WithTrailingTrivia(node.GetTrailingTrivia());
        return document.WithSyntaxRoot(root.ReplaceNode(node, replacement));
    }

    /// <summary>
    /// Replace new System.Random() with new WorkflowRandom().
    /// </summary>
    private static async Task<Document> ReplaceWithWorkflowRandom(
        Document document, SyntaxNode node, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);
        if (root == null) return document;

        var replacement = SyntaxFactory.IdentifierName("WorkflowRandom")
            .WithLeadingTrivia(node.GetLeadingTrivia())
            .WithTrailingTrivia(node.GetTrailingTrivia());

        var newRoot = root.ReplaceNode(node, replacement);
        return document.WithSyntaxRoot(newRoot);
    }
}
