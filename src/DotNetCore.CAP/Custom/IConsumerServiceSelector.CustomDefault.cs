using DotNetCore.CAP.Abstractions;
using DotNetCore.CAP.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace DotNetCore.CAP.Custom
{
    /// <summary>
    /// A Custom <see cref="T:DotNetCore.CAP.Abstractions.IConsumerServiceSelector"/> implementation
    /// </summary>
    public class CustomDefaultConsumerServiceSelector : IConsumerServiceSelector
    {
        private readonly CapOptions _capOptions;
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// since this class be designed as a Singleton service, the following two list must be thread-safe
        /// </summary>
        private readonly ConcurrentDictionary<string, List<CustomRegexExecuteDescriptor<ConsumerExecutorDescriptor>>> _asterriskList;
        private readonly ConcurrentDictionary<string, List<CustomRegexExecuteDescriptor<ConsumerExecutorDescriptor>>> _poundList;

        public CustomDefaultConsumerServiceSelector(IServiceProvider serviceProvider,CapOptions capOptions)
        {
            _asterriskList = new ConcurrentDictionary<string, List<CustomRegexExecuteDescriptor<ConsumerExecutorDescriptor>>>();
            _poundList = new ConcurrentDictionary<string, List<CustomRegexExecuteDescriptor<ConsumerExecutorDescriptor>>>();
        }

        public IReadOnlyList<ConsumerExecutorDescriptor> SelectCandidates()
        {
            var executorDescriptorList = new List<ConsumerExecutorDescriptor>();

            executorDescriptorList.AddRange(FindConsumersFromInterfaceTypes(_serviceProvider));

            executorDescriptorList.AddRange(FindConsumersFromControllerTypes());

            return executorDescriptorList;
        }



        public ConsumerExecutorDescriptor SelectBestCandidate(string key, IReadOnlyList<ConsumerExecutorDescriptor> executorDescriptor)
        {
            var result = MatchUsingName(key, executorDescriptor);
            if(result !=null)
            {
                return result;
            }

            //[*] match with regex, i.e. foo.*.abc
            result = MatchAsteriskUsingRegex(key, executorDescriptor);
            if(result !=null)
            {
                return result;
            }
            //[#] match regex, i.e. foo.#
            result = MatchPoundUsingRegex(key, executorDescriptor);
            return result;
        }

        private ConsumerExecutorDescriptor MatchPoundUsingRegex(string key, IReadOnlyList<ConsumerExecutorDescriptor> executeDescriptor)
        {
            var group = executeDescriptor.First().Attribute.Group;
            if (_asterriskList.TryGetValue(group, out var tmpList))
            {
                tmpList = executeDescriptor.Where(x => x.Attribute.Name.IndexOf('*') >= 0)
                    .Select(x => new CustomRegexExecuteDescriptor<ConsumerExecutorDescriptor>
                    {
                        Name = ("^" + x.Attribute.Name + "$").Replace("#", "[0-9_a-zA-Z\\.]+"),

                        Descriptor = x
                    }).ToList();
                _asterriskList.TryAdd(group, tmpList);
            }

            foreach (var red in tmpList)
            {
                if (Regex.IsMatch(key, red.Name, RegexOptions.Singleline))
                {
                    return red.Descriptor;
                }
            }

            return null;
        }

        private ConsumerExecutorDescriptor MatchAsteriskUsingRegex(string key, IReadOnlyList<ConsumerExecutorDescriptor> executorDescriptor)
        {
            var group = executorDescriptor.First().Attribute.Group;
            if(_asterriskList.TryGetValue(group,out var tmpList))
            {
                tmpList = executorDescriptor.Where(x => x.Attribute.Name.IndexOf('*') >= 0)
                    .Select(x => new CustomRegexExecuteDescriptor<ConsumerExecutorDescriptor>
                    {
                        Name = ("^" + x.Attribute.Name + "$").Replace("*", "[0-9_a-zA-Z]+").Replace(".", "\\."),
                        Descriptor = x
                    }).ToList();
                _asterriskList.TryAdd(group, tmpList);
            }

            foreach(var red in tmpList)
            {
                if(Regex.IsMatch(key,red.Name, RegexOptions.Singleline))
                {
                    return red.Descriptor;
                }
            }

            return null;
        }

        private ConsumerExecutorDescriptor MatchUsingName(string key, IReadOnlyList<ConsumerExecutorDescriptor> executorDescriptor)
        {
            return executorDescriptor.FirstOrDefault(x => x.MethodInfo.Name == key);
        }

        private IEnumerable<ConsumerExecutorDescriptor> FindConsumersFromControllerTypes()
        {
            var executorDesciptorList = new List<ConsumerExecutorDescriptor>();

            var types = Assembly.GetEntryAssembly().ExportedTypes;
            foreach(var type in types)
            {
                var typeinfo = type.GetTypeInfo();
                if(Helper.IsController(typeinfo))
                {
                    executorDesciptorList.AddRange(GetTopicAttributesDescription(typeinfo));
                }
            }

            return executorDesciptorList;


        }

        private IEnumerable<ConsumerExecutorDescriptor> FindConsumersFromInterfaceTypes(IServiceProvider provider)
        {
            var executorDescriptorList = new List<ConsumerExecutorDescriptor>();

            using (var scoped = provider.CreateScope())
            {
                var scopedProvider = scoped.ServiceProvider;
                var consumerServices = scopedProvider.GetServices<ICapSubscribe>();
                foreach(var service in consumerServices)
                {
                    var typeinfo = service.GetType().GetTypeInfo();
                    if(!typeof(ICapSubscribe).GetTypeInfo().IsAssignableFrom(typeinfo))
                    {
                        continue;
                    }

                    executorDescriptorList.AddRange(GetTopicAttributesDescription(typeinfo));
                }
            }

            return executorDescriptorList;
        }

        private IEnumerable<ConsumerExecutorDescriptor> GetTopicAttributesDescription(TypeInfo typeinfo)
        {
            foreach(var method in typeinfo.DeclaredMethods)
            {
                var topicAttr = method.GetCustomAttributes<TopicAttribute>(true);
                var topicAttributes = topicAttr as IList<TopicAttribute> ?? topicAttr.ToList();

                if(!topicAttributes.Any())
                {
                    continue;
                }

                foreach(var attr in topicAttributes)
                {
                    if(attr.Group ==null)
                    {
                        attr.Group = _capOptions.DefaultGroup + "." + _capOptions.Version;
                    }
                    else
                    {
                        attr.Group = attr.Group + "." + _capOptions.Version;
                    }

                    yield return InitDescriptor(attr, method, typeinfo);
                }
            }
        }

        private ConsumerExecutorDescriptor InitDescriptor(TopicAttribute attr, MethodInfo method, TypeInfo typeinfo)
        {
            return new ConsumerExecutorDescriptor() { ImplTypeInfo = typeinfo, MethodInfo = method, Attribute = attr };
        }

      
        private class CustomRegexExecuteDescriptor<T>
        {
            public string Name { get; set; }
            public T Descriptor { get; set; }
        }
    }

   
}
