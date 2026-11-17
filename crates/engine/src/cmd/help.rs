//! Help → community and project links (The AllCraft website, web app and GitHub repository).
//! The commands return the URL; the frontend opens it.

use serde_json::{Value, json};

use super::{CommandSpec, Result, always, cmd};

/// The app's id and public URLs.
pub const APP_ID: &str = "the-allcraft";
pub const DISCORD_URL: &str = "https://github.com/thepixopik/vectorcraft";
pub const WEBSITE_URL: &str = "https://the-allcraft.pages.dev";

/// This app's page on the website.
pub fn app_page_url() -> String {
    WEBSITE_URL.to_string()
}

/// This app's source repository.
pub fn github_url() -> String {
    "https://github.com/thepixopik/vectorcraft".to_string()
}

pub fn specs() -> Vec<CommandSpec> {
    vec![
        cmd!(query "help.discord", "Community on GitHub", ["Help"], None, "{} → {url} project community", always, |_, _| url(DISCORD_URL.into())),
        cmd!(query "help.website", "The AllCraft Website", ["Help"], None, "{} → {url}", always, |_, _| url(WEBSITE_URL.into())),
        cmd!(query "help.appPage", "The AllCraft Web App", ["Help"], None, "{} → {url} this app's page", always, |_, _| url(app_page_url())),
        cmd!(query "help.github", "Source Code on GitHub", ["Help"], None, "{} → {url} source code, issues and releases", always, |_, _| url(github_url())),
        cmd!(query "help.links", "Links", [], None, "{} → {discord, website, appPage, github}", always, |_, _| {
            Ok(json!({ "discord": DISCORD_URL, "website": WEBSITE_URL, "appPage": app_page_url(), "github": github_url() }))
        }),
    ]
}

fn url(u: String) -> Result<Value> {
    Ok(json!({ "url": u }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Session;

    #[test]
    fn links() {
        let mut s = Session::new();
        assert_eq!(s.execute("help.discord", &json!({})).unwrap()["url"], "https://github.com/thepixopik/vectorcraft");
        assert_eq!(s.execute("help.appPage", &json!({})).unwrap()["url"], "https://the-allcraft.pages.dev");
        assert_eq!(s.execute("help.github", &json!({})).unwrap()["url"], "https://github.com/thepixopik/vectorcraft");
        assert_eq!(s.execute("help.links", &json!({})).unwrap()["website"], "https://the-allcraft.pages.dev");
    }
}
