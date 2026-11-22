//! `cutover`: swap bucket names so clients that address the bucket by name
//! read the migrated data. Check-only unless `--execute --yes`.

use super::{connect, sh};
use crate::cli::{CutoverArgs, Global};
use crate::error::{Kind, fail};
use crate::influx::{BucketToken, Dbrp};
use crate::output::Outcome;
use serde::Serialize;
use std::fmt::Write as _;
use tracing::{error, info, warn};

#[derive(Serialize)]
struct Report {
    executed: bool,
    source: String,
    source_id: String,
    target: String,
    target_id: String,
    archive_name: String,
    steps: Vec<String>,
    /// Tokens bound to the source bucket's ID: after the swap they still point at the archived bucket.
    tokens_bound_to_source: Vec<BucketToken>,
    /// Tokens bound to the target bucket's ID: after the swap they point at the production name.
    tokens_bound_to_target: Vec<BucketToken>,
    /// InfluxQL (v1) mappings of the source bucket: they keep pointing at the archived bucket.
    dbrp_mappings_of_source: Vec<Dbrp>,
}

pub fn run(g: &Global, a: &CutoverArgs) -> anyhow::Result<Outcome> {
    let archive = a
        .archive_name
        .clone()
        .unwrap_or_else(|| format!("{}_pre_migration", a.source_bucket));
    if a.source_bucket == a.target_bucket
        || archive == a.source_bucket
        || archive == a.target_bucket
    {
        return Err(fail(
            Kind::Usage,
            "source, target and archive name must all differ",
        )
        .into());
    }
    if a.execute && !a.yes {
        return Err(
            fail(Kind::Refused, "--execute renames buckets and needs --yes")
                .hint("run without --execute first to review the plan, then add --execute --yes")
                .into(),
        );
    }
    let client = connect(g)?;
    let source = client.bucket(&a.source_bucket)?;
    let target = client.bucket(&a.target_bucket)?;
    if client.find_bucket(&archive)?.is_some() {
        return Err(fail(
            Kind::Refused,
            format!("a bucket named `{archive}` already exists"),
        )
        .hint("choose another --archive-name")
        .into());
    }
    let tokens_src = client.tokens_for_bucket(&source.id)?;
    let tokens_tgt = client.tokens_for_bucket(&target.id)?;
    let dbrps = client.dbrps_for_bucket(&source.org_id, &source.id)?;

    let steps = vec![
        format!(
            "stop writers of `{}` (e.g. Telegraf) so no point lands between the renames",
            source.name
        ),
        format!("rename `{}` -> `{archive}`", source.name),
        format!("rename `{}` -> `{}`", target.name, source.name),
        "start the writers again".to_owned(),
    ];
    let mut report = Report {
        executed: false,
        source: source.name.clone(),
        source_id: source.id.clone(),
        target: target.name.clone(),
        target_id: target.id.clone(),
        archive_name: archive.clone(),
        steps,
        tokens_bound_to_source: tokens_src,
        tokens_bound_to_target: tokens_tgt,
        dbrp_mappings_of_source: dbrps,
    };

    if a.execute {
        info!(from = %source.name, to = %archive, "renaming source bucket");
        client.rename_bucket(&source.id, &archive)?;
        info!(from = %target.name, to = %source.name, "renaming target bucket");
        if let Err(e) = client.rename_bucket(&target.id, &source.name) {
            error!(error = %e, "second rename failed, restoring the original source name");
            if let Err(e2) = client.rename_bucket(&source.id, &source.name) {
                return Err(fail(
                    Kind::Internal,
                    format!(
                        "rename of `{}` failed ({e}) and restoring `{}` failed too ({e2})",
                        target.name, source.name
                    ),
                )
                .hint(format!(
                    "rename bucket id {} back to `{}` manually",
                    source.id, source.name
                ))
                .into());
            }
            return Err(e.context("cutover rolled back, nothing changed"));
        }
        report.executed = true;
        info!("cutover done");
    }

    let mut human = String::new();
    let _ = writeln!(
        human,
        "{}: `{}` ({}) -> `{archive}`, `{}` ({}) -> `{}`",
        if report.executed {
            "Cutover done"
        } else {
            "Cutover plan (nothing changed)"
        },
        report.source,
        report.source_id,
        report.target,
        report.target_id,
        report.source
    );
    for (i, s) in report.steps.iter().enumerate() {
        let _ = writeln!(human, "  {}. {s}", i + 1);
    }

    let mut o = Outcome::ok(&report, human);
    for t in &report.tokens_bound_to_source {
        let msg = format!(
            "token {} ({}) is bound to the old bucket ID with {}; after the cutover it reaches `{archive}`, not the migrated data",
            t.id,
            if t.description.is_empty() {
                "no description"
            } else {
                &t.description
            },
            t.actions.join("+")
        );
        warn!("{msg}");
        o.warnings.push(msg);
    }
    for d in &report.dbrp_mappings_of_source {
        o.warnings.push(format!(
            "InfluxQL mapping {} ({}/{}) points at the old bucket ID; remap it to {} for InfluxQL clients (e.g. Grafana InfluxQL data sources)",
            d.id, d.database, d.retention_policy, report.target_id
        ));
    }
    if !report.executed {
        o.next_steps.push(format!(
            "loxinflux-migration-helper cutover --source-bucket {} --target-bucket {} --archive-name {} --execute --yes",
            sh(&a.source_bucket),
            sh(&a.target_bucket),
            sh(&archive)
        ));
    } else {
        if !report.tokens_bound_to_source.is_empty() {
            o.next_steps.push(format!(
                "create tokens for bucket ID {} and replace the ones listed in the warnings",
                report.target_id
            ));
        }
        o.next_steps.push(format!(
            "keep `{archive}` until you are sure, then delete it with `influx bucket delete --name {}`",
            sh(&archive)
        ));
    }
    Ok(o)
}
