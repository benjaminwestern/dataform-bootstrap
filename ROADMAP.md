# Roadmap

## Roadmap and Future Enhancements
- [ ] Add testing for collectors and generators
- [ ] Add testing for CLI and Utils
- [ ] Refine the logging and error handling for better user experience
- [ ] Upgrade the Generator to support Policy Tags, more Partition Options, Clustering, Primary Keys, Foreign Keys, Table Tags, Labels, etc.
- [ ] Refine SQL Simlarity and SQL Differencing to better de-duplicate actions and reduce the number of SQL statements generated.
- [ ] Implement comprehensive output formatting system
  - [ ] Create a `Report` class that can handle the output formatting and generation
  - [ ] Basic output modes (minimal, detailed, JSON)
  - [ ] Enhanced output formats (YAML, HTML, Markdown)
  - [ ] Interactive terminal output with progress bars
  - [ ] Customisable report templates
  - [ ] Export capabilities for various platforms
- [ ] Upgrade dataform filename and filepath generation to be more flexible and customisable. i.e. `{{schema}}_{{table}}_{{type}}.sql` or `{{schema}}/{{table}}/{{type}}.sql` etc.
- [ ] Give a more detailed and comprehensive overview of the generated Dataform models and the status of the generation process. `Migrated Failed` and `Migrated Successful` is not enough.
- [ ] Make sure I haven't reinvented the wheel for the CLI, Logging, and Error Handling. If there are better libraries or tools that can be used, use them.

## Pipe-dream Enhancements
- [ ] Create a `Dataform` model generator that can generate Dataform models from BigQuery SQL + Dataform Config
- [ ] The ability to generate `Dataform` models from `dbt` models.